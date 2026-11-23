package flatjsonl

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

// ResolveConfig describes versioned record resolution.
//
// When enabled, rows that share the same identity (values of IdentityKeys) are
// considered versions of the same logical record. Only the version with the
// highest RevisionKey value is written to the outputs, so the resulting data
// does not depend on the order inputs are read. Rows with equal identity and
// revision but different content are reported as locatable conflicts.
//
// Resolution buffers distinct record versions in memory before emitting the
// final rows, so it is not suitable for unbounded data sets.
type ResolveConfig struct {
	IdentityKeys   []string `json:"identityKeys" yaml:"identityKeys" description:"Keys that identify a logical record, e.g. [\".id\"]."`
	RevisionKey    string   `json:"revisionKey" yaml:"revisionKey" description:"Key with numeric record revision, highest revision wins, e.g. \".rev\"."`
	ConflictReport string   `json:"conflictReport" yaml:"conflictReport" description:"Optional file path to write conflicts as JSON lines, conflicts are logged to STDERR if empty."`
}

func (c ResolveConfig) validate() error {
	if len(c.IdentityKeys) == 0 {
		return errors.New("resolve: at least one identity key is required")
	}

	if c.RevisionKey == "" {
		return errors.New("resolve: revision key is required")
	}

	for _, k := range c.IdentityKeys {
		if k == c.RevisionKey {
			return fmt.Errorf("resolve: revision key %q must not be an identity key", k)
		}
	}

	return nil
}

// requiredKeys lists keys that must be among included keys for resolution to work.
func (c ResolveConfig) requiredKeys() []string {
	return append(append(make([]string, 0, len(c.IdentityKeys)+1), c.IdentityKeys...), c.RevisionKey)
}

// ensureIncludedKeys appends required keys that are missing from includeKeys.
func ensureIncludedKeys(includeKeys, required []string) []string {
	for _, k := range required {
		found := false

		for _, ik := range includeKeys {
			if ik == k {
				found = true

				break
			}
		}

		if !found {
			includeKeys = append(includeKeys, k)
		}
	}

	return includeKeys
}

// maxResolveConflicts is the maximum number of conflicts kept in memory for reporting.
const maxResolveConflicts = 10000

// maxLoggedConflicts is the maximum number of conflicts logged to STDERR when no report file is configured.
const maxLoggedConflicts = 50

// inputSpan maps a range of row sequences to an input, so that a row can be
// located in its source.
type inputSpan struct {
	name     string
	firstSeq int64 // Sequence of the first row read from this input.
	lastSeq  int64 // Sequence of the last row read from this input.
	offset   int64 // Number of skipped offset lines.
}

// revEntry is a buffered representative of a record version.
type revEntry struct {
	hash   uint64
	values []Value
	seq    int64
}

// recordEntry holds all distinct versions seen for a single identity.
type recordEntry struct {
	revs map[float64]*revEntry
}

// conflictSide describes one of the conflicting rows.
type conflictSide struct {
	Location    string `json:"location"`
	ContentHash string `json:"contentHash"`
}

// resolveConflict describes a collision of rows that share identity and
// revision, but differ in content.
type resolveConflict struct {
	Identity      map[string]string `json:"identity"`
	Revision      float64           `json:"revision"`
	Kept          conflictSide      `json:"kept"`
	Dropped       conflictSide      `json:"dropped"`
	DifferingKeys []string          `json:"differingKeys"`
}

// recordResolver buffers versioned rows and emits a single winning row per
// identity, selection is deterministic and independent of input read order.
type recordResolver struct {
	p *Processor

	identityIdx []int
	revisionIdx int
	sequenceIdx int // Index of "._sequence" meta column, -1 if absent, excluded from content hash.

	records map[string]*recordEntry
	spans   []inputSpan
	maxSeq  int64

	received    int64 // Rows that carried identity and revision.
	duplicates  int64 // Rows identical to an already buffered version.
	passthrough int64 // Rows without identity or revision, written to output as is.
	conflicts   []resolveConflict
	nConflicts  int64 // Total number of conflicts, including evicted from storage.

	hasher *hasher
	buf    strings.Builder
}

// newRecordResolver creates a resolver and maps configured keys to value indexes.
func newRecordResolver(p *Processor) (*recordResolver, error) {
	cfg := p.cfg.Resolve

	r := &recordResolver{
		p:           p,
		records:     map[string]*recordEntry{},
		revisionIdx: -1,
		sequenceIdx: -1,
		hasher:      newHasher(),
	}

	indexes := make(map[string]int, len(p.includeKeys))
	for k, i := range p.includeKeys {
		indexes[p.ck(k)] = i
	}

	for _, k := range cfg.IdentityKeys {
		i, ok := indexes[p.ck(k)]
		if !ok {
			return nil, fmt.Errorf("resolve identity key %q is not among included keys", k)
		}

		r.identityIdx = append(r.identityIdx, i)
	}

	i, ok := indexes[p.ck(cfg.RevisionKey)]
	if !ok {
		return nil, fmt.Errorf("resolve revision key %q is not among included keys", cfg.RevisionKey)
	}

	r.revisionIdx = i

	if i, ok := indexes["._sequence"]; ok {
		r.sequenceIdx = i
	}

	return r, nil
}

// beginInput registers the input that is about to be read, its sequence range
// is open-ended until endInput is called.
func (r *recordResolver) beginInput(name string, firstSeq, offset int64) {
	r.spans = append(r.spans, inputSpan{
		name:     name,
		firstSeq: firstSeq,
		lastSeq:  math.MaxInt64,
		offset:   offset,
	})
}

// endInput closes the sequence range of the current input.
func (r *recordResolver) endInput(lastSeq int64) {
	if len(r.spans) == 0 {
		return
	}

	s := &r.spans[len(r.spans)-1]
	s.lastSeq = lastSeq

	if s.lastSeq < s.firstSeq {
		// No rows were read from this input.
		r.spans = r.spans[:len(r.spans)-1]
	}
}

// locate returns "input:line" for a row sequence.
func (r *recordResolver) locate(seq int64) string {
	for i := len(r.spans) - 1; i >= 0; i-- {
		s := r.spans[i]
		if seq >= s.firstSeq && seq <= s.lastSeq {
			return fmt.Sprintf("%s:%d", s.name, seq-s.firstSeq+1+s.offset)
		}
	}

	return fmt.Sprintf("seq:%d", seq)
}

// receiveRow buffers a versioned row, it replaces Writer.ReceiveRow when
// resolution is enabled. Rows are copied where necessary, the incoming slice
// is reused by the caller.
func (r *recordResolver) receiveRow(seq int64, values []Value) error {
	if seq > r.maxSeq {
		r.maxSeq = seq
	}

	id, ok := r.identityOf(values)
	if !ok {
		return r.passRow(seq, values, "missing identity")
	}

	rev, ok := revisionOf(values[r.revisionIdx])
	if !ok {
		return r.passRow(seq, values, "missing or non-numeric revision")
	}

	r.received++

	hash := r.contentHash(values)

	rec := r.records[id]
	if rec == nil {
		rec = &recordEntry{revs: map[float64]*revEntry{}}
		r.records[id] = rec
	}

	e := rec.revs[rev]
	if e == nil {
		rec.revs[rev] = &revEntry{hash: hash, values: cloneValues(values), seq: seq}

		return nil
	}

	if e.hash == hash {
		r.duplicates++

		return nil
	}

	r.addConflict(rev, e, hash, values, seq)

	// Deterministic, read-order independent choice: lowest content hash wins.
	if hash < e.hash {
		e.hash = hash
		e.values = cloneValues(values)
		e.seq = seq
	}

	return nil
}

// passRow writes a row that can not participate in resolution directly to the output.
func (r *recordResolver) passRow(seq int64, values []Value, reason string) error {
	r.passthrough++

	if r.passthrough <= 10 {
		r.p.Log(fmt.Sprintf("resolve: passing through row at %s: %s", r.locate(seq), reason))
	}

	return r.p.w.ReceiveRow(seq, values)
}

// identityOf builds an unambiguous identity string from identity key values.
func (r *recordResolver) identityOf(values []Value) (string, bool) {
	r.buf.Reset()

	for _, idx := range r.identityIdx {
		v := values[idx]
		if v.Type == TypeAbsent || v.Type == TypeNull {
			return "", false
		}

		s := canonicalValue(v)

		// Length-prefixed to keep concatenation unambiguous.
		r.buf.WriteString(strconv.Itoa(len(s)))
		r.buf.WriteByte(':')
		r.buf.WriteString(s)
	}

	return r.buf.String(), true
}

// contentHash hashes the canonical form of all values of a row, excluding the
// "._sequence" meta column that reflects read order rather than content.
func (r *recordResolver) contentHash(values []Value) uint64 {
	r.buf.Reset()

	for i, v := range values {
		if i == r.sequenceIdx {
			continue
		}

		s := canonicalValue(v)
		r.buf.WriteString(strconv.Itoa(len(s)))
		r.buf.WriteByte(':')
		r.buf.WriteString(s)
	}

	return r.hasher.hashBytes([]byte(r.buf.String()))
}

// canonicalValue renders value in a canonical typed form, so that equal
// logical values (e.g. 1 and 1.0) have equal forms.
func canonicalValue(v Value) string {
	switch v.Type {
	case TypeFloat:
		return "f" + strconv.FormatFloat(v.Number, 'g', -1, 64)
	case TypeString:
		return "s" + v.String
	case TypeBool:
		return "b" + strconv.FormatBool(v.Bool)
	case TypeNull:
		return "n"
	case TypeAbsent:
		return "a"
	default:
		return "?" + v.Format()
	}
}

// revisionOf extracts a numeric revision from a value.
func revisionOf(v Value) (float64, bool) {
	var rev float64

	switch v.Type {
	case TypeFloat:
		rev = v.Number
	case TypeString:
		f, err := strconv.ParseFloat(strings.TrimSpace(v.String), 64)
		if err != nil {
			return 0, false
		}

		rev = f
	default:
		return 0, false
	}

	if math.IsNaN(rev) || math.IsInf(rev, 0) {
		return 0, false
	}

	return rev, true
}

// addConflict records a collision of two rows with same identity and revision,
// but different content.
func (r *recordResolver) addConflict(rev float64, e *revEntry, hash uint64, values []Value, seq int64) {
	r.nConflicts++

	if len(r.conflicts) >= maxResolveConflicts {
		return
	}

	newSide := conflictSide{Location: r.locate(seq), ContentHash: strconv.FormatUint(hash, 16)}
	oldSide := conflictSide{Location: r.locate(e.seq), ContentHash: strconv.FormatUint(e.hash, 16)}

	c := resolveConflict{
		Identity:      r.identityMap(e.values),
		Revision:      rev,
		DifferingKeys: r.differingKeys(e.values, values),
	}

	// Kept side is the deterministic winner with the lowest content hash.
	if hash < e.hash {
		c.Kept, c.Dropped = newSide, oldSide
	} else {
		c.Kept, c.Dropped = oldSide, newSide
	}

	r.conflicts = append(r.conflicts, c)
}

// identityMap renders identity key values of a row for reporting.
func (r *recordResolver) identityMap(values []Value) map[string]string {
	m := make(map[string]string, len(r.identityIdx))

	for _, idx := range r.identityIdx {
		m[r.p.keys[idx].original] = values[idx].Format()
	}

	return m
}

// differingKeys lists original key names with different canonical values in two rows.
func (r *recordResolver) differingKeys(a, b []Value) []string {
	var keys []string

	for i := range a {
		if i == r.sequenceIdx {
			continue
		}

		if canonicalValue(a[i]) != canonicalValue(b[i]) {
			keys = append(keys, r.p.keys[i].original)
		}
	}

	return keys
}

// flush emits a single winning row per identity to the outputs and reports
// conflicts. Winners are emitted in deterministic order sorted by identity.
func (r *recordResolver) flush() error {
	type winner struct {
		id  string
		rev float64
		e   *revEntry
	}

	winners := make([]winner, 0, len(r.records))

	for id, rec := range r.records {
		w := winner{id: id}
		first := true

		for rev, e := range rec.revs {
			if first || rev > w.rev {
				w.rev, w.e = rev, e
				first = false
			}
		}

		winners = append(winners, w)
	}

	// Deterministic output order independent of input read order.
	sort.Slice(winners, func(i, j int) bool { return winners[i].id < winners[j].id })

	seq := r.maxSeq

	for _, w := range winners {
		seq++

		if r.sequenceIdx >= 0 {
			w.e.values[r.sequenceIdx] = Value{
				Type:      TypeFloat,
				Number:    float64(seq),
				RawNumber: strconv.FormatInt(seq, 10),
			}
		}

		if err := r.p.w.ReceiveRow(seq, w.e.values); err != nil {
			return fmt.Errorf("failed to write resolved row: %w", err)
		}
	}

	if err := r.reportConflicts(); err != nil {
		return err
	}

	r.p.Log(fmt.Sprintf("resolved %d records from %d rows (duplicates: %d, conflicts: %d, passed through: %d)",
		len(winners), r.received, r.duplicates, r.nConflicts, r.passthrough))

	return nil
}

// reportConflicts writes conflicts to the configured report file or logs them.
func (r *recordResolver) reportConflicts() error {
	if r.nConflicts == 0 {
		return nil
	}

	if r.nConflicts > int64(len(r.conflicts)) {
		r.p.Log(fmt.Sprintf("resolve: only first %d of %d conflicts are reported", len(r.conflicts), r.nConflicts))
	}

	report := r.p.cfg.Resolve.ConflictReport

	if report == "" {
		for i, c := range r.conflicts {
			if i >= maxLoggedConflicts {
				r.p.Log(fmt.Sprintf("resolve: ... and %d more conflicts, set resolve conflictReport to dump all to a file",
					len(r.conflicts)-i))

				break
			}

			b, err := json.Marshal(c)
			if err != nil {
				return fmt.Errorf("failed to marshal resolve conflict: %w", err)
			}

			r.p.Log("resolve conflict: " + string(b))
		}

		return nil
	}

	f, err := os.Create(report)
	if err != nil {
		return fmt.Errorf("failed to create resolve conflict report %s: %w", report, err)
	}

	enc := json.NewEncoder(f)

	for _, c := range r.conflicts {
		if err := enc.Encode(c); err != nil {
			_ = f.Close()

			return fmt.Errorf("failed to write resolve conflict report: %w", err)
		}
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close resolve conflict report: %w", err)
	}

	r.p.Log(fmt.Sprintf("resolve: %d conflicts written to %s", r.nConflicts, report))

	return nil
}

func cloneValues(values []Value) []Value {
	c := make([]Value, len(values))
	copy(c, values)

	return c
}
