/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2026 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package minio

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"sort"

	"github.com/minio/minio-go/v7/pkg/s3utils"
)

// resumePartLayout derives the part layout of an upload that is already in
// progress. The size of the first stored part is authoritative: the caller's
// current object size, or a PartSize computed from it, must not move the
// boundaries of parts the server has already accepted.
//
// Only layouts the server can complete are accepted. Every stored non-final
// part must have exactly the recorded size, the recorded size must be inside
// this client's upload limits, and the current object must fit in the client's
// maximum part count. Anything else is rejected instead of being sent as an
// arbitrary mix of part sizes.
func (l UploadLimits) resumePartLayout(objectSize int64, configuredPartSize uint64, parts map[int]ObjectPart) (totalPartsCount int, partSize, lastPartSize int64, err error) {
	if objectSize <= 0 {
		return 0, 0, 0, errInvalidArgument("Cannot resume a multipart upload for an empty object.")
	}
	if objectSize > l.maxObjectSize() {
		return 0, 0, 0, errEntityTooLarge(objectSize, l.maxObjectSize(), "", "")
	}

	if len(parts) == 0 {
		return l.optimalPartInfo(objectSize, configuredPartSize)
	}

	// Part 1 is the only stored part whose size is always the upload's part
	// size. A later part may be the short final part, so using it as the chunk
	// size would silently move every boundary.
	recordedSize := int64(0)
	if part, ok := parts[1]; ok {
		recordedSize = part.Size
	}
	if recordedSize == 0 && configuredPartSize == 0 {
		return 0, 0, 0, errInvalidArgument("Cannot determine the recorded part size: part 1 is missing. Set PartSize to the size used by the original upload.")
	}
	if recordedSize == 0 {
		if configuredPartSize > uint64(l.maxPartSize()) {
			return 0, 0, 0, errInvalidArgument(fmt.Sprintf("Part size %d exceeds the maximum allowed part size %d.", configuredPartSize, l.maxPartSize()))
		}
		recordedSize = int64(configuredPartSize)
	}
	if recordedSize < 0 {
		return 0, 0, 0, errInvalidArgument("Recorded part size cannot be negative.")
	}
	if recordedSize > l.maxPartSize() {
		return 0, 0, 0, errInvalidArgument(fmt.Sprintf("Recorded part size %d exceeds the maximum allowed part size %d.", recordedSize, l.maxPartSize()))
	}

	total := int64(1)
	if objectSize > recordedSize {
		total = 1 + (objectSize-1)/recordedSize
	}
	if total > l.maxPartsCount() {
		return 0, 0, 0, errInvalidArgument(fmt.Sprintf("Recorded part size %d needs %d parts for an object of %d bytes, more than the allowed %d.", recordedSize, total, objectSize, l.maxPartsCount()))
	}

	last := objectSize - (total-1)*recordedSize
	if total > 1 && recordedSize < l.minPartSize() {
		return 0, 0, 0, errInvalidArgument(fmt.Sprintf("Recorded part size %d is smaller than the allowed minimum %d.", recordedSize, l.minPartSize()))
	}

	for number, part := range parts {
		if number < 1 || int64(number) > total {
			return 0, 0, 0, errInvalidArgument(fmt.Sprintf("Stored part number %d is outside the resumed upload layout of %d parts.", number, total))
		}
		expected := recordedSize
		if int64(number) == total {
			expected = last
		}
		if part.Size != expected {
			return 0, 0, 0, errInvalidArgument(fmt.Sprintf("Stored part %d has size %d, expected %d for the recorded part size %d.", number, part.Size, expected, recordedSize))
		}
	}

	return int(total), recordedSize, last, nil
}

// resumeSection returns a reader for one part of a seekable source.
func resumeSection(reader io.Reader, offset, length int64) (io.Reader, error) {
	if readerAt, ok := reader.(io.ReaderAt); ok {
		return io.NewSectionReader(readerAt, offset, length), nil
	}
	if seeker, ok := reader.(io.Seeker); ok {
		if _, err := seeker.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
		return io.LimitReader(reader, length), nil
	}
	return nil, errInvalidArgument("Resuming a multipart upload requires a reader that implements io.ReaderAt or io.Seeker.")
}

// putObjectMultipartResume continues an upload using the part size recorded by
// the server. Parts already stored are not uploaded again; the remaining parts
// are read from their position in the source and the upload is then completed.
//
// The upload is deliberately not aborted on failure. Aborting would destroy
// every part that was successfully stored and make a later resume impossible.
func (c *Client) putObjectMultipartResume(ctx context.Context, bucketName, objectName string, reader io.Reader, size int64,
	opts PutObjectOptions,
) (info UploadInfo, err error) {
	if err = s3utils.CheckValidBucketName(bucketName); err != nil {
		return UploadInfo{}, err
	}
	if err = s3utils.CheckValidObjectName(objectName); err != nil {
		return UploadInfo{}, err
	}
	if opts.UploadID == "" {
		return UploadInfo{}, errInvalidArgument("UploadID cannot be empty when resuming a multipart upload.")
	}
	if size <= 0 {
		return UploadInfo{}, errInvalidArgument("Resuming a multipart upload requires a positive object size.")
	}
	if _, ok := reader.(io.ReaderAt); !ok {
		if _, ok := reader.(io.Seeker); !ok {
			return UploadInfo{}, errInvalidArgument("Resuming a multipart upload requires a reader that implements io.ReaderAt or io.Seeker.")
		}
	}

	storedParts, err := c.listObjectParts(ctx, bucketName, objectName, opts.UploadID)
	if err != nil {
		return UploadInfo{}, err
	}

	totalPartsCount, partSize, lastPartSize, err := c.limits.resumePartLayout(size, opts.PartSize, storedParts)
	if err != nil {
		return UploadInfo{}, err
	}
	if totalPartsCount == 0 {
		return UploadInfo{}, errInvalidArgument("Cannot resume a multipart upload without parts.")
	}

	// An upload started by an older client may not carry the checksum this
	// client would add by default. Do not fail such a resume for that reason;
	// an explicitly requested checksum, however, must be present on every
	// stored part or completion cannot succeed.
	if len(storedParts) > 0 && opts.AutoChecksum.IsSet() {
		allHaveChecksum := true
		for _, part := range storedParts {
			if part.Checksum(opts.AutoChecksum) == "" {
				allHaveChecksum = false
				break
			}
		}
		if !allHaveChecksum {
			if opts.Checksum.IsSet() {
				return UploadInfo{}, errInvalidArgument("Stored parts do not have the requested checksum; they cannot be combined with Checksum.")
			}
			opts.AutoChecksum = ChecksumNone
		}
	}

	uploadedParts := make(map[int]ObjectPart, totalPartsCount)
	for number, part := range storedParts {
		uploadedParts[number] = part
	}

	withChecksum := c.trailingHeaderSupport && opts.AutoChecksum.IsSet()
	for number := 1; number <= totalPartsCount; number++ {
		if _, ok := uploadedParts[number]; ok {
			continue
		}

		partLength := partSize
		if number == totalPartsCount {
			partLength = lastPartSize
		}
		offset := int64(number-1) * partSize
		section, rerr := resumeSection(reader, offset, partLength)
		if rerr != nil {
			return UploadInfo{}, rerr
		}

		var md5Base64 string
		if opts.SendContentMd5 {
			md5Hash := c.md5Hasher()
			if _, herr := io.Copy(md5Hash, section); herr != nil {
				md5Hash.Close()
				return UploadInfo{}, herr
			}
			md5Base64 = base64.StdEncoding.EncodeToString(md5Hash.Sum(nil))
			md5Hash.Close()

			section, rerr = resumeSection(reader, offset, partLength)
			if rerr != nil {
				return UploadInfo{}, rerr
			}
		}
		section = newHook(section, opts.Progress)

		trailer := make(http.Header, 1)
		if withChecksum {
			crc := opts.AutoChecksum.Hasher()
			trailer.Set(opts.AutoChecksum.Key(), base64.StdEncoding.EncodeToString(crc.Sum(nil)))
			section = newHashReaderWrapper(section, crc, func(hash []byte) {
				trailer.Set(opts.AutoChecksum.Key(), base64.StdEncoding.EncodeToString(hash))
			})
		}

		objPart, uerr := c.uploadPart(ctx, uploadPartParams{
			bucketName:   bucketName,
			objectName:   objectName,
			uploadID:     opts.UploadID,
			reader:       section,
			partNumber:   number,
			md5Base64:    md5Base64,
			size:         partLength,
			sse:          opts.ServerSideEncryption,
			streamSha256: !opts.DisableContentSha256,
			customHeader: make(http.Header),
			trailer:      trailer,
		})
		if uerr != nil {
			return UploadInfo{}, uerr
		}
		uploadedParts[number] = objPart
	}

	allParts := make([]ObjectPart, 0, totalPartsCount)
	complete := completeMultipartUpload{Parts: make([]CompletePart, 0, totalPartsCount)}
	for number := 1; number <= totalPartsCount; number++ {
		part, ok := uploadedParts[number]
		if !ok {
			return UploadInfo{}, errInvalidArgument(fmt.Sprintf("Missing part number %d", number))
		}
		allParts = append(allParts, part)
		complete.Parts = append(complete.Parts, CompletePart{
			ETag:              part.ETag,
			PartNumber:        part.PartNumber,
			ChecksumCRC32:     part.ChecksumCRC32,
			ChecksumCRC32C:    part.ChecksumCRC32C,
			ChecksumSHA1:      part.ChecksumSHA1,
			ChecksumSHA256:    part.ChecksumSHA256,
			ChecksumCRC64NVME: part.ChecksumCRC64NVME,
			ChecksumMD5:       part.ChecksumMD5,
			ChecksumSHA512:    part.ChecksumSHA512,
			ChecksumXXHash64:  part.ChecksumXXHash64,
			ChecksumXXHash3:   part.ChecksumXXHash3,
			ChecksumXXHash128: part.ChecksumXXHash128,
		})
	}
	sort.Sort(completedParts(complete.Parts))

	uploadID := opts.UploadID
	opts = completeUploadOpts(opts)
	applyAutoChecksum(&opts, allParts)
	uploadInfo, err := c.completeMultipartUpload(ctx, bucketName, objectName, uploadID, complete, opts)
	if err != nil {
		return UploadInfo{}, err
	}
	uploadInfo.Size = size
	return uploadInfo, nil
}
