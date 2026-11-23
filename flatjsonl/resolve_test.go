package flatjsonl_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vearutop/flatjsonl/flatjsonl"
)

const resolveOldSnapshot = `{"id": 1, "rev": 1, "name": "alpha-old", "v": 10}
{"id": 2, "rev": 2, "name": "beta", "v": 20}
{"id": 3, "rev": 5, "name": "gamma-old", "v": 30}
{"id": 4, "rev": 1, "name": "delta", "v": 40}
{"id": 6, "name": "no-revision"}
{"rev": 9, "name": "no-identity"}
`

const resolveNewSnapshot = `{"id": 1, "rev": 3, "name": "alpha-new", "v": 11}
{"id": 2, "rev": 1, "name": "beta-ancient", "v": 21}
{"id": 3, "rev": 5, "name": "gamma-new", "v": 31}
{"id": 1, "rev": 3, "name": "alpha-new", "v": 11}
{"id": 5, "rev": 2, "name": "epsilon", "v": 50}
`

type resolveRun struct {
	sqliteRows  []map[string]string
	parquetRows []map[string]string
	conflicts   []resolveConflictEntry
	logs        string
}

type resolveConflictEntry struct {
	Identity map[string]string `json:"identity"`
	Revision float64           `json:"revision"`
	Kept     struct {
		Location    string `json:"location"`
		ContentHash string `json:"contentHash"`
	} `json:"kept"`
	Dropped struct {
		Location    string `json:"location"`
		ContentHash string `json:"contentHash"`
	} `json:"dropped"`
	DifferingKeys []string `json:"differingKeys"`
}

func writeTestFile(t *testing.T, fn, content string) {
	t.Helper()

	require.NoError(t, os.WriteFile(fn, []byte(content), 0o600))
}

func runResolve(t *testing.T, dir string, inputs ...string) resolveRun {
	t.Helper()

	require.NoError(t, os.MkdirAll(dir, 0o755))

	sqliteFile := filepath.Join(dir, "out.sqlite")
	parquetFile := filepath.Join(dir, "out.parquet")
	csvFile := filepath.Join(dir, "out.csv")
	reportFile := filepath.Join(dir, "conflicts.jsonl")

	f := flatjsonl.Flags{}
	f.SQLite = sqliteFile
	f.Parquet = parquetFile
	f.CSV = csvFile
	f.SQLTable = "resolved"
	f.Concurrency = 1

	cfg := flatjsonl.Config{
		Resolve: &flatjsonl.ResolveConfig{
			IdentityKeys:   []string{".id"},
			RevisionKey:    ".rev",
			ConflictReport: reportFile,
		},
	}

	in := make([]flatjsonl.Input, 0, len(inputs))
	for _, fn := range inputs {
		in = append(in, flatjsonl.Input{FileName: fn})
	}

	proc, err := flatjsonl.NewProcessor(f, cfg, in...)
	require.NoError(t, err)

	var logs strings.Builder
	proc.Log = func(args ...any) {
		logs.WriteString(fmt.Sprintln(args...))
	}

	require.NoError(t, proc.Process())

	var conflicts []resolveConflictEntry

	if rb, err := os.ReadFile(reportFile); err == nil {
		for _, line := range strings.Split(strings.TrimSpace(string(rb)), "\n") {
			var c resolveConflictEntry
			require.NoError(t, json.Unmarshal([]byte(line), &c))

			conflicts = append(conflicts, c)
		}
	}

	sqliteRows := readSQLiteRows(t, sqliteFile, "resolved")
	parquetRows := readParquetRows(t, parquetFile)

	// SQLite and Parquet must hold the same effective records.
	assert.ElementsMatch(t, sqliteRows, parquetRows,
		"SQLite and Parquet effective records differ")

	return resolveRun{
		sqliteRows:  sqliteRows,
		parquetRows: parquetRows,
		conflicts:   conflicts,
		logs:        logs.String(),
	}
}

func readSQLiteRows(t *testing.T, fn, table string) []map[string]string {
	t.Helper()

	db, err := sql.Open("sqlite", fn)
	require.NoError(t, err)

	defer func() { require.NoError(t, db.Close()) }()

	rows, err := db.Query(`SELECT * FROM "` + table + `"`)
	require.NoError(t, err)

	defer func() { require.NoError(t, rows.Close()) }()

	cols, err := rows.Columns()
	require.NoError(t, err)

	var res []map[string]string

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))

		for i := range vals {
			ptrs[i] = &vals[i]
		}

		require.NoError(t, rows.Scan(ptrs...))

		m := map[string]string{}

		for i, c := range cols {
			if c == "_seq_id" {
				continue
			}

			switch v := vals[i].(type) {
			case nil:
				// NULL, same as absent in Parquet.
			case int64:
				m[c] = strconv.FormatInt(v, 10)
			case float64:
				m[c] = strconv.FormatFloat(v, 'g', -1, 64)
			case string:
				m[c] = v
			case []byte:
				m[c] = string(v)
			default:
				t.Fatalf("unexpected column type %T", v)
			}
		}

		res = append(res, m)
	}

	require.NoError(t, rows.Err())

	return res
}

func rowsByID(rows []map[string]string) map[string]map[string]string {
	res := map[string]map[string]string{}

	for _, r := range rows {
		res[r[".id"]] = r
	}

	return res
}

func TestResolve_mixedSnapshotsOrderIndependent(t *testing.T) {
	dir := t.TempDir()

	oldFile := filepath.Join(dir, "resolve_old.jsonl")
	newFile := filepath.Join(dir, "resolve_new.jsonl")
	writeTestFile(t, oldFile, resolveOldSnapshot)
	writeTestFile(t, newFile, resolveNewSnapshot)

	runs := []resolveRun{
		runResolve(t, filepath.Join(dir, "run1"), oldFile, newFile),
		runResolve(t, filepath.Join(dir, "run2"), newFile, oldFile),
	}

	for i, run := range runs {
		require.Len(t, run.sqliteRows, 7, "run %d: unexpected row count", i)
		require.Len(t, run.parquetRows, 7, "run %d: unexpected row count", i)

		byID := rowsByID(run.sqliteRows)

		// Highest revision wins regardless of input order.
		assert.Equal(t, map[string]string{".id": "1", ".rev": "3", ".name": "alpha-new", ".v": "11"}, byID["1"])
		assert.Equal(t, map[string]string{".id": "2", ".rev": "2", ".name": "beta", ".v": "20"}, byID["2"])
		assert.Equal(t, map[string]string{".id": "4", ".rev": "1", ".name": "delta", ".v": "40"}, byID["4"])
		assert.Equal(t, map[string]string{".id": "5", ".rev": "2", ".name": "epsilon", ".v": "50"}, byID["5"])

		// Rows without identity or revision are passed through.
		assert.Equal(t, map[string]string{".id": "6", ".name": "no-revision"}, byID["6"])

		var noIdentity []map[string]string

		for _, r := range run.sqliteRows {
			if _, ok := r[".id"]; !ok {
				noIdentity = append(noIdentity, r)
			}
		}

		require.Len(t, noIdentity, 1)
		assert.Equal(t, map[string]string{".rev": "9", ".name": "no-identity"}, noIdentity[0])

		// Same identity and revision with different content is a locatable conflict.
		require.Len(t, run.conflicts, 1, "run %d: expected exactly one conflict", i)

		c := run.conflicts[0]
		assert.Equal(t, map[string]string{".id": "3"}, c.Identity)
		assert.Equal(t, 5.0, c.Revision)
		assert.ElementsMatch(t, []string{".name", ".v"}, c.DifferingKeys)
		assert.ElementsMatch(t,
			[]string{oldFile + ":3", newFile + ":3"},
			[]string{c.Kept.Location, c.Dropped.Location},
			"conflict must point at both colliding rows")

		// The final row for the conflicting identity matches the kept side of the report.
		expectedByLocation := map[string]map[string]string{
			oldFile + ":3": {".id": "3", ".rev": "5", ".name": "gamma-old", ".v": "30"},
			newFile + ":3": {".id": "3", ".rev": "5", ".name": "gamma-new", ".v": "31"},
		}
		assert.Equal(t, expectedByLocation[c.Kept.Location], byID["3"],
			"run %d: final row does not match the kept side of the conflict report", i)

		assert.Contains(t, run.logs, "duplicates: 1")
		assert.Contains(t, run.logs, "conflicts: 1")
		assert.Contains(t, run.logs, "passed through: 2")
	}

	// Final values must not depend on the order inputs are read.
	assert.ElementsMatch(t, runs[0].sqliteRows, runs[1].sqliteRows,
		"SQLite final rows depend on input order")
	assert.ElementsMatch(t, runs[0].parquetRows, runs[1].parquetRows,
		"Parquet effective records depend on input order")
	assert.Equal(t, runs[0].conflicts, runs[1].conflicts,
		"conflict report depends on input order")
}

func TestResolve_duplicateSnapshots(t *testing.T) {
	dir := t.TempDir()

	newFile := filepath.Join(dir, "resolve_new.jsonl")
	writeTestFile(t, newFile, resolveNewSnapshot)

	run := runResolve(t, dir, newFile, newFile)

	// Each row of the duplicated input is an exact copy, no conflicts expected.
	require.Len(t, run.sqliteRows, 4)
	assert.Empty(t, run.conflicts)
	assert.Contains(t, run.logs, "duplicates: 6")
	assert.Contains(t, run.logs, "conflicts: 0")

	byID := rowsByID(run.sqliteRows)
	assert.Equal(t, map[string]string{".id": "3", ".rev": "5", ".name": "gamma-new", ".v": "31"}, byID["3"])
}

func TestResolve_addSequence(t *testing.T) {
	dir := t.TempDir()

	input := filepath.Join(dir, "input.jsonl")
	writeTestFile(t, input, `{"id": 1, "rev": 1, "name": "a"}
{"id": 2, "rev": 1, "name": "b"}
`)

	csvFile := filepath.Join(dir, "out.csv")

	f := flatjsonl.Flags{}
	f.AddSequence = true
	f.CSV = csvFile
	f.Concurrency = 1

	cfg := flatjsonl.Config{
		Resolve: &flatjsonl.ResolveConfig{
			IdentityKeys: []string{".id"},
			RevisionKey:  ".rev",
		},
	}

	// Read the same input twice: rows only differ in the synthetic ._sequence
	// column, so they must be recognized as duplicates, not conflicts.
	proc, err := flatjsonl.NewProcessor(f, cfg,
		flatjsonl.Input{FileName: input}, flatjsonl.Input{FileName: input})
	require.NoError(t, err)

	var logs strings.Builder
	proc.Log = func(args ...any) {
		logs.WriteString(fmt.Sprintln(args...))
	}

	require.NoError(t, proc.Process())

	assert.Contains(t, logs.String(), "duplicates: 2")
	assert.Contains(t, logs.String(), "conflicts: 0")

	b, err := os.ReadFile(csvFile)
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	require.Len(t, lines, 3) // Header + 2 resolved rows.

	assert.Equal(t, `._sequence,.id,.rev,.name`, lines[0])

	// Resolved rows are renumbered densely after all read sequences (1-4).
	assert.Equal(t, `5,1,1,a`, lines[1])
	assert.Equal(t, `6,2,1,b`, lines[2])
}

func TestResolve_flags(t *testing.T) {
	dir := t.TempDir()

	input := filepath.Join(dir, "input.jsonl")
	writeTestFile(t, input, `{"id": 1, "rev": 1, "name": "old"}
{"id": 1, "rev": 2, "name": "new"}
`)

	csvFile := filepath.Join(dir, "out.csv")

	f := flatjsonl.Flags{}
	f.ResolveIdentity = ".id"
	f.ResolveRevision = ".rev"
	f.CSV = csvFile
	f.Concurrency = 1

	proc, err := flatjsonl.NewProcessor(f, flatjsonl.Config{}, flatjsonl.Input{FileName: input})
	require.NoError(t, err)
	require.NoError(t, proc.Process())

	b, err := os.ReadFile(csvFile)
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	require.Len(t, lines, 2)
	assert.Equal(t, `1,2,new`, lines[1])
}

func TestResolve_includeKeysAutoExtend(t *testing.T) {
	dir := t.TempDir()

	input := filepath.Join(dir, "input.jsonl")
	writeTestFile(t, input, `{"id": 1, "rev": 1, "name": "old", "extra": "x"}
{"id": 1, "rev": 2, "name": "new", "extra": "y"}
`)

	csvFile := filepath.Join(dir, "out.csv")

	f := flatjsonl.Flags{}
	f.CSV = csvFile
	f.Concurrency = 1

	cfg := flatjsonl.Config{
		IncludeKeys: []string{".name"},
		Resolve: &flatjsonl.ResolveConfig{
			IdentityKeys: []string{".id"},
			RevisionKey:  ".rev",
		},
	}

	proc, err := flatjsonl.NewProcessor(f, cfg, flatjsonl.Input{FileName: input})
	require.NoError(t, err)
	require.NoError(t, proc.Process())

	b, err := os.ReadFile(csvFile)
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	require.Len(t, lines, 2)

	// Identity and revision keys are automatically included.
	assert.Equal(t, `.name,.id,.rev`, lines[0])
	assert.Equal(t, `new,1,2`, lines[1])
}

func TestResolve_concurrent(t *testing.T) {
	dir := t.TempDir()

	var sb strings.Builder
	for i := 1; i <= 1000; i++ {
		sb.WriteString(fmt.Sprintf(`{"id": %d, "rev": 1, "name": "n%d-old"}`, i, i) + "\n")
		sb.WriteString(fmt.Sprintf(`{"id": %d, "rev": 2, "name": "n%d-new"}`, i, i) + "\n")
	}

	input := filepath.Join(dir, "input.jsonl")
	writeTestFile(t, input, sb.String())

	sqliteFile := filepath.Join(dir, "out.sqlite")

	f := flatjsonl.Flags{}
	f.SQLite = sqliteFile
	f.SQLTable = "resolved"
	f.Concurrency = 8

	cfg := flatjsonl.Config{
		Resolve: &flatjsonl.ResolveConfig{
			IdentityKeys: []string{".id"},
			RevisionKey:  ".rev",
		},
	}

	proc, err := flatjsonl.NewProcessor(f, cfg,
		flatjsonl.Input{FileName: input}, flatjsonl.Input{FileName: input})
	require.NoError(t, err)

	var logs strings.Builder
	proc.Log = func(args ...any) {
		logs.WriteString(fmt.Sprintln(args...))
	}

	require.NoError(t, proc.Process())

	rows := readSQLiteRows(t, sqliteFile, "resolved")
	require.Len(t, rows, 1000)

	for _, r := range rows {
		assert.Equal(t, "2", r[".rev"])
		assert.Equal(t, "n"+r[".id"]+"-new", r[".name"])
	}

	assert.Contains(t, logs.String(), "duplicates: 2000")
	assert.Contains(t, logs.String(), "conflicts: 0")
}

func TestResolve_validation(t *testing.T) {
	_, err := flatjsonl.NewProcessor(flatjsonl.Flags{}, flatjsonl.Config{
		Resolve: &flatjsonl.ResolveConfig{RevisionKey: ".rev"},
	})
	require.EqualError(t, err, "resolve: at least one identity key is required")

	_, err = flatjsonl.NewProcessor(flatjsonl.Flags{}, flatjsonl.Config{
		Resolve: &flatjsonl.ResolveConfig{IdentityKeys: []string{".id"}},
	})
	require.EqualError(t, err, "resolve: revision key is required")

	_, err = flatjsonl.NewProcessor(flatjsonl.Flags{}, flatjsonl.Config{
		Resolve: &flatjsonl.ResolveConfig{IdentityKeys: []string{".id"}, RevisionKey: ".id"},
	})
	require.EqualError(t, err, `resolve: revision key ".id" must not be an identity key`)
}

func TestResolve_missingKeysError(t *testing.T) {
	dir := t.TempDir()

	input := filepath.Join(dir, "input.jsonl")
	writeTestFile(t, input, `{"id": 1, "rev": 1}
`)

	f := flatjsonl.Flags{}
	f.CSV = filepath.Join(dir, "out.csv")
	f.Concurrency = 1

	cfg := flatjsonl.Config{
		ExcludeKeys: []string{".rev"},
		Resolve: &flatjsonl.ResolveConfig{
			IdentityKeys: []string{".id"},
			RevisionKey:  ".rev",
		},
	}

	proc, err := flatjsonl.NewProcessor(f, cfg, flatjsonl.Input{FileName: input})
	require.NoError(t, err)

	err = proc.Process()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `resolve revision key ".rev" is not among included keys`)
}
