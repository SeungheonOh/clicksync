package store

import (
	"errors"
	"reflect"
	"testing"

	"clicksync/internal/model"
	"clicksync/internal/publication"
)

func TestOutputRefChunksCoverLargeLookupExactly(t *testing.T) {
	refs := outputRefFixtures(outputRefQueryChunkSize*2 + 17)
	var visited []publication.OutputRef
	var chunkSizes []int
	if err := forEachOutputRefChunk(
		refs,
		func(chunk []publication.OutputRef) error {
			chunkSizes = append(chunkSizes, len(chunk))
			visited = append(visited, chunk...)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if want := []int{
		outputRefQueryChunkSize,
		outputRefQueryChunkSize,
		17,
	}; !reflect.DeepEqual(chunkSizes, want) {
		t.Fatalf("chunk sizes = %v, want %v", chunkSizes, want)
	}
	if !reflect.DeepEqual(visited, refs) {
		t.Fatal("chunk traversal omitted, duplicated, or reordered an output reference")
	}
}

func TestOutputRefDeduplicationPrecedesChunking(t *testing.T) {
	refs := outputRefFixtures(outputRefQueryChunkSize + 2)
	refs[outputRefQueryChunkSize] = refs[0]
	unique := uniqueOutputRefs(refs)
	if len(unique) != len(refs)-1 {
		t.Fatalf("unique output references = %d, want %d", len(unique), len(refs)-1)
	}
	if unique[0] != refs[0] ||
		unique[outputRefQueryChunkSize] != refs[outputRefQueryChunkSize+1] {
		t.Fatal("cross-chunk output reference deduplication changed first-seen order")
	}
}

func TestOutputRefChunkParametersPreserveCrossChunkBinaryPairs(t *testing.T) {
	refs := outputRefFixtures(outputRefQueryChunkSize + 1)
	offset := 0
	if err := forEachOutputRefChunk(
		refs,
		func(chunk []publication.OutputRef) error {
			if len(chunk) == 0 || len(chunk) > outputRefQueryChunkSize {
				t.Fatalf("invalid chunk length %d", len(chunk))
			}
			hashes, indexes := outputRefQueryParameters(chunk)
			if len(hashes) != len(chunk) || len(indexes) != len(chunk) {
				t.Fatalf(
					"parameter lengths hashes=%d indexes=%d chunk=%d",
					len(hashes),
					len(indexes),
					len(chunk),
				)
			}
			for index, ref := range chunk {
				if hashes[index] != string(ref.Hash[:]) ||
					indexes[index] != ref.Index ||
					ref != refs[offset+index] {
					t.Fatalf(
						"parameter pair %d:%d differs across chunk boundary",
						offset,
						index,
					)
				}
			}
			offset += len(chunk)
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if offset != len(refs) {
		t.Fatalf("visited %d references, want %d", offset, len(refs))
	}

	stop := errors.New("stop")
	calls := 0
	err := forEachOutputRefChunk(refs, func([]publication.OutputRef) error {
		calls++
		if calls == 2 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || calls != 2 {
		t.Fatalf("chunk error propagation calls=%d err=%v", calls, err)
	}
}

func TestCandidatePublicationChunksPreserveFirstSeenDeduplication(t *testing.T) {
	ids := make([]uint64, candidatePublicationQueryChunkSize+3)
	for index := range ids {
		ids[index] = uint64(index + 1)
	}
	ids[candidatePublicationQueryChunkSize] = 1
	ids[candidatePublicationQueryChunkSize+1] = 2
	ids[candidatePublicationQueryChunkSize+2] = 99_999
	unique := uniquePublicationIDs(ids)
	if len(unique) != candidatePublicationQueryChunkSize+1 {
		t.Fatalf(
			"unique candidate IDs = %d, want %d",
			len(unique),
			candidatePublicationQueryChunkSize+1,
		)
	}
	for index := 0; index < candidatePublicationQueryChunkSize; index++ {
		if unique[index] != uint64(index+1) {
			t.Fatalf("candidate ID %d = %d", index, unique[index])
		}
	}
	if unique[len(unique)-1] != 99_999 {
		t.Fatalf("last candidate ID = %d, want 99999", unique[len(unique)-1])
	}
	for start := 0; start < len(unique); start += candidatePublicationQueryChunkSize {
		end := min(start+candidatePublicationQueryChunkSize, len(unique))
		if size := end - start; size < 1 || size > candidatePublicationQueryChunkSize {
			t.Fatalf("candidate publication chunk size = %d", size)
		}
	}
}

func outputRefFixtures(count int) []publication.OutputRef {
	refs := make([]publication.OutputRef, count)
	for index := range refs {
		var hash model.Hash32
		for byteIndex := range hash {
			// Includes NUL, quotes, backslashes, and non-UTF-8 bytes that the
			// native driver must escape while expanding query parameters.
			hash[byteIndex] = byte(index*37 + byteIndex*53)
		}
		refs[index] = publication.OutputRef{
			Hash:  hash,
			Index: uint32(index*7 + 3),
		}
	}
	return refs
}
