package content

import (
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
)

// ID is an identifier of content in content-addressable storage.
type ID string

// Prefix returns a one-character prefix of a content ID or an empty string.
func (i ID) Prefix() ID {
	if i.HasPrefix() {
		return i[0:1]
	}

	return ""
}

// HasPrefix determines if the given ID has a non-empty prefix.
func (i ID) HasPrefix() bool {
	return len(i)%2 == 1
}

// Info is an information about a single piece of content managed by Manager.
type Info interface {
	GetContentID() ID
	GetPackBlobID() blob.ID
	GetTimestampSeconds() int64
	Timestamp() time.Time
	GetOriginalLength() uint32
	GetPackedLength() uint32
	GetPackOffset() uint32
	GetDeleted() bool
	GetFormatVersion() byte
	GetCompressionHeaderID() compression.HeaderID
	GetEncryptionKeyID() byte
}

type deletedInfo struct {
	Info
	deletedTime int64
}

func (d *deletedInfo) GetDeleted() bool {
	return true
}

func (d *deletedInfo) GetTimestampSeconds() int64 {
	return d.deletedTime
}

// InfoStruct is an implementation of Info based on a structure.
type InfoStruct struct {
	ContentID           ID                   `json:"contentID"`
	PackBlobID          blob.ID              `json:"packFile,omitempty"`
	TimestampSeconds    int64                `json:"time"`
	OriginalLength      uint32               `json:"originalLength"`
	PackedLength        uint32               `json:"length"`
	PackOffset          uint32               `json:"packOffset,omitempty"`
	Deleted             bool                 `json:"deleted"`
	FormatVersion       byte                 `json:"formatVersion"`
	CompressionHeaderID compression.HeaderID `json:"compression,omitempty"`
	EncryptionKeyID     byte                 `json:"encryptionKeyID,omitempty"`
}

// GetContentID implements the Info interface.
func (i *InfoStruct) GetContentID() ID { return i.ContentID }

// GetPackBlobID implements the Info interface.
func (i *InfoStruct) GetPackBlobID() blob.ID { return i.PackBlobID }

// GetTimestampSeconds implements the Info interface.
func (i *InfoStruct) GetTimestampSeconds() int64 { return i.TimestampSeconds }

// GetOriginalLength implements the Info interface.
func (i *InfoStruct) GetOriginalLength() uint32 { return i.OriginalLength }

// GetPackedLength implements the Info interface.
func (i *InfoStruct) GetPackedLength() uint32 { return i.PackedLength }

// GetPackOffset implements the Info interface.
func (i *InfoStruct) GetPackOffset() uint32 { return i.PackOffset }

// GetDeleted implements the Info interface.
func (i *InfoStruct) GetDeleted() bool { return i.Deleted }

// GetFormatVersion implements the Info interface.
func (i *InfoStruct) GetFormatVersion() byte { return i.FormatVersion }

// GetCompressionHeaderID implements the Info interface.
func (i *InfoStruct) GetCompressionHeaderID() compression.HeaderID { return i.CompressionHeaderID }

// GetEncryptionKeyID implements the Info interface.
func (i *InfoStruct) GetEncryptionKeyID() byte { return i.EncryptionKeyID }

// Timestamp implements the Info interface.
func (i *InfoStruct) Timestamp() time.Time {
	return time.Unix(i.GetTimestampSeconds(), 0)
}

// ToInfoStruct converts the provided Info to *InfoStruct.
func ToInfoStruct(i Info) *InfoStruct {
	if is, ok := i.(*InfoStruct); ok {
		return is
	}

	return &InfoStruct{
		ContentID:           i.GetContentID(),
		PackBlobID:          i.GetPackBlobID(),
		TimestampSeconds:    i.GetTimestampSeconds(),
		OriginalLength:      i.GetOriginalLength(),
		PackedLength:        i.GetPackedLength(),
		PackOffset:          i.GetPackOffset(),
		Deleted:             i.GetDeleted(),
		FormatVersion:       i.GetFormatVersion(),
		CompressionHeaderID: i.GetCompressionHeaderID(),
		EncryptionKeyID:     i.GetEncryptionKeyID(),
	}
}
