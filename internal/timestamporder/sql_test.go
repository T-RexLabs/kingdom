package timestamporder

import (
	"database/sql"
	"sort"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestSQLiteOrderingPreservesRFC3339NanoPrecision(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open SQLite database: %v", err)
	}
	defer db.Close()

	t.Run("order by instant", func(t *testing.T) {
		const wholeSecond = "2026-09-25T10:00:00Z"
		const laterFraction = "2026-09-25T10:00:00.1Z"
		var latest string
		query := `SELECT timestamp FROM (SELECT ? AS timestamp UNION ALL SELECT ?) ORDER BY ` + OrderBySQL("timestamp", true) + ` LIMIT 1`
		if err := db.QueryRow(query, wholeSecond, laterFraction).Scan(&latest); err != nil {
			t.Fatalf("order timestamps in SQLite: %v", err)
		}
		if latest != laterFraction {
			t.Fatalf("latest timestamp=%q, want %q", latest, laterFraction)
		}
	})

	t.Run("preserve nanoseconds at second boundary", func(t *testing.T) {
		const before = "2026-09-25T10:00:00.999999999Z"
		const after = "2026-09-25T10:00:01Z"
		var latest string
		query := `SELECT timestamp FROM (SELECT ? AS timestamp UNION ALL SELECT ?) ORDER BY ` + OrderBySQL("timestamp", true) + ` LIMIT 1`
		if err := db.QueryRow(query, before, after).Scan(&latest); err != nil {
			t.Fatalf("order timestamps at second boundary: %v", err)
		}
		if latest != after {
			t.Fatalf("latest timestamp=%q, want %q", latest, after)
		}
	})

	t.Run("compare lease across fractions", func(t *testing.T) {
		const leaseExpiry = "2026-09-25T10:00:00.11Z"
		const now = "2026-09-25T10:00:00.1Z"
		var leaseSeconds, nowSeconds int64
		var leaseFraction, nowFraction string
		partsQuery := `WITH lease(lease_expires_at, now_at) AS (VALUES (?, ?)) SELECT ` + secondsSQL("lease_expires_at") + `, ` + fractionalNanosecondsSQL("lease_expires_at") + `, ` + secondsSQL("now_at") + `, ` + fractionalNanosecondsSQL("now_at") + ` FROM lease`
		if err := db.QueryRow(partsQuery, leaseExpiry, now).Scan(&leaseSeconds, &leaseFraction, &nowSeconds, &nowFraction); err != nil {
			t.Fatalf("extract timestamp comparison parts: %v", err)
		}
		if leaseSeconds != nowSeconds || leaseFraction != "110000000" || nowFraction != "100000000" {
			t.Fatalf("timestamp comparison parts lease=(%d,%q), now=(%d,%q)", leaseSeconds, leaseFraction, nowSeconds, nowFraction)
		}
		var expired bool
		query := `WITH lease(lease_expires_at, now_at) AS (VALUES (?, ?)) SELECT ` + ComparisonSQL("lease_expires_at", "now_at", "<=") + ` FROM lease`
		if err := db.QueryRow(query, leaseExpiry, now).Scan(&expired); err != nil {
			t.Fatalf("compare lease timestamps in SQLite: %v", err)
		}
		if expired {
			t.Fatalf("lease expiry %q was stale at %q", leaseExpiry, now)
		}
	})

	t.Run("compare offset timestamps by instant", func(t *testing.T) {
		const laterInstant = "2026-09-25T10:00:00.11Z"
		const earlierWithOffset = "2026-09-25T11:00:00.1+01:00"
		var later bool
		query := `WITH timestamps(later_at, earlier_at) AS (VALUES (?, ?)) SELECT ` + ComparisonSQL("later_at", "earlier_at", ">") + ` FROM timestamps`
		if err := db.QueryRow(query, laterInstant, earlierWithOffset).Scan(&later); err != nil {
			t.Fatalf("compare offset timestamps in SQLite: %v", err)
		}
		if !later {
			t.Fatalf("%q should be later than %q", laterInstant, earlierWithOffset)
		}
	})
}

func TestSQLiteOrderBySQLMatchesRFC3339NanoChronology(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open SQLite database: %v", err)
	}
	defer db.Close()

	values := []string{
		"2026-09-25T10:00:00Z",
		"2026-09-25T10:00:00.1Z",
		"2026-09-25T11:00:00.1+01:00",
		"2026-09-25T10:00:00.11Z",
		"2026-09-25T10:00:00.123456789+05:45",
		"2026-09-25T04:15:00.123456789Z",
		"2026-09-25T10:00:00.123456789-03:30",
		"2026-09-25T13:30:00.123456789Z",
		"2026-09-25T09:59:59.999999999Z",
		"0000-02-28T23:59:59.999999999Z",
		"0000-02-29T00:00:00Z",
		"1900-02-28T23:59:59.999999999Z",
		"1900-03-01T00:00:00Z",
		"2000-02-28T23:59:59.999999999Z",
		"2000-02-29T00:00:00Z",
		"1969-12-31T23:59:59.999999999Z",
		"1970-01-01T00:00:00Z",
		"0000-01-01T00:00:00+01:00",
		"0000-01-01T00:00:00Z",
		"9999-12-31T23:59:59.999999999-01:00",
		"9999-12-31T23:59:59.999999999Z",
	}
	instants := make(map[string]time.Time, len(values))
	for _, value := range values {
		instant, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			t.Fatalf("parse timestamp %q: %v", value, err)
		}
		instants[value] = instant
	}

	for _, descending := range []bool{false, true} {
		direction := "ascending"
		if descending {
			direction = "descending"
		}
		t.Run(direction, func(t *testing.T) {
			query := timestampOrderQuery(stringValues(values), descending)
			if got, want := strings.Count(query, "?"), len(values); got != want {
				t.Fatalf("order query has %d placeholders, want %d", got, want)
			}

			rows, err := db.Query(query, stringValues(values)...)
			if err != nil {
				t.Fatalf("order RFC3339Nano timestamps: %v", err)
			}
			var got []string
			for rows.Next() {
				var value string
				if err := rows.Scan(&value); err != nil {
					_ = rows.Close()
					t.Fatalf("scan ordered timestamp: %v", err)
				}
				got = append(got, value)
			}
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				t.Fatalf("iterate ordered timestamps: %v", err)
			}
			if err := rows.Close(); err != nil {
				t.Fatalf("close ordered timestamp rows: %v", err)
			}

			want := append([]string(nil), values...)
			sort.Slice(want, func(i, j int) bool {
				comparison := instants[want[i]].Compare(instants[want[j]])
				if comparison == 0 {
					comparison = strings.Compare(want[i], want[j])
				}
				if descending {
					return comparison > 0
				}
				return comparison < 0
			})
			if !equalStrings(got, want) {
				t.Fatalf("ordered timestamps=%v, want chronological order %v", got, want)
			}
		})
	}
}

func TestSQLiteOrderBySQLHandlesNullAndEmptyValues(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open SQLite database: %v", err)
	}
	defer db.Close()

	values := []any{nil, "", "2026-09-25T10:00:00Z", "2026-09-25T10:00:00.1Z"}
	for _, tc := range []struct {
		descending bool
		want       []sql.NullString
	}{
		{want: []sql.NullString{{}, {String: "", Valid: true}, {String: "2026-09-25T10:00:00Z", Valid: true}, {String: "2026-09-25T10:00:00.1Z", Valid: true}}},
		{descending: true, want: []sql.NullString{{String: "2026-09-25T10:00:00.1Z", Valid: true}, {String: "2026-09-25T10:00:00Z", Valid: true}, {String: "", Valid: true}, {}}},
	} {
		query := timestampOrderQuery(values, tc.descending)
		if got, want := strings.Count(query, "?"), len(values); got != want {
			t.Fatalf("order query has %d placeholders, want %d", got, want)
		}
		rows, err := db.Query(query, values...)
		if err != nil {
			t.Fatalf("order null and empty timestamps: %v", err)
		}
		var got []sql.NullString
		for rows.Next() {
			var value sql.NullString
			if err := rows.Scan(&value); err != nil {
				_ = rows.Close()
				t.Fatalf("scan ordered timestamp: %v", err)
			}
			got = append(got, value)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatalf("iterate ordered timestamps: %v", err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close ordered timestamp rows: %v", err)
		}
		if !equalNullableStrings(got, tc.want) {
			t.Fatalf("ordered null/empty timestamps=%v, want %v", got, tc.want)
		}
	}
}

func TestSQLiteComparisonSQLMatchesRFC3339NanoInstantsAndBindCount(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open SQLite database: %v", err)
	}
	defer db.Close()

	pairs := []struct {
		name  string
		left  string
		right string
	}{
		{name: "whole second before later fraction", left: "2026-09-25T10:00:00Z", right: "2026-09-25T10:00:00.1Z"},
		{name: "eleven hundredths after one tenth", left: "2026-09-25T10:00:00.11Z", right: "2026-09-25T10:00:00.1Z"},
		{name: "offset-equivalent instants", left: "2026-09-25T10:00:00.1Z", right: "2026-09-25T11:00:00.1+01:00"},
		{name: "positive offset minutes", left: "2026-09-25T10:00:00.123456789+05:45", right: "2026-09-25T04:15:00.123456789Z"},
		{name: "negative offset minutes", left: "2026-09-25T10:00:00.123456789-03:30", right: "2026-09-25T13:30:00.123456789Z"},
		{name: "offset crossing midnight", left: "2026-09-25T00:15:00+01:00", right: "2026-09-24T23:15:00Z"},
		{name: "nanosecond before next second", left: "2026-09-25T10:00:00.999999999Z", right: "2026-09-25T11:00:00+01:00"},
		{name: "year zero leap day", left: "0000-02-28T23:59:59.999999999Z", right: "0000-02-29T00:00:00Z"},
		{name: "non-leap century boundary", left: "1900-02-28T23:59:59.999999999Z", right: "1900-03-01T00:00:00Z"},
		{name: "leap century boundary", left: "2000-02-28T23:59:59.999999999Z", right: "2000-02-29T00:00:00Z"},
		{name: "before Unix epoch", left: "1969-12-31T23:59:59.999999999Z", right: "1970-01-01T00:00:00Z"},
		{name: "year zero offset boundary", left: "0000-01-01T00:00:00+01:00", right: "0000-01-01T00:00:00Z"},
		{name: "year 9999 offset boundary", left: "9999-12-31T23:59:59.999999999-01:00", right: "9999-12-31T23:59:59.999999999Z"},
	}
	operators := []string{"<", "<=", ">", ">=", "="}

	for _, pair := range pairs {
		pair := pair
		t.Run(pair.name, func(t *testing.T) {
			left, err := time.Parse(time.RFC3339Nano, pair.left)
			if err != nil {
				t.Fatalf("parse left timestamp %q: %v", pair.left, err)
			}
			right, err := time.Parse(time.RFC3339Nano, pair.right)
			if err != nil {
				t.Fatalf("parse right timestamp %q: %v", pair.right, err)
			}

			for _, operator := range operators {
				t.Run(operator, func(t *testing.T) {
					expression := ComparisonSQL("left_at", "?", operator)
					if got := strings.Count(expression, "?"); got != 1 {
						t.Fatalf("comparison expression has %d RHS placeholders, want exactly one: %s", got, expression)
					}
					query := `WITH timestamps(left_at) AS (VALUES (?)) SELECT ` + expression + ` FROM timestamps`
					if got, want := strings.Count(query, "?"), 2; got != want {
						t.Fatalf("comparison query has %d placeholders, want %d", got, want)
					}

					var got bool
					if err := db.QueryRow(query, pair.left, pair.right).Scan(&got); err != nil {
						t.Fatalf("compare %q %s %q: %v", pair.left, operator, pair.right, err)
					}
					if want := compareTimes(left, right, operator); got != want {
						t.Fatalf("%q %s %q = %t, want %t", pair.left, operator, pair.right, got, want)
					}
				})
			}
		})
	}
}

func TestSQLiteComparisonSQLTreatsNullAndEmptyTimestampsAsUnordered(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open SQLite database: %v", err)
	}
	defer db.Close()

	valid := "2026-09-25T10:00:00Z"
	for _, tc := range []struct {
		name  string
		left  any
		right any
	}{
		{name: "null left", left: nil, right: valid},
		{name: "empty left", left: "", right: valid},
		{name: "null right", left: valid, right: nil},
		{name: "empty right", left: valid, right: ""},
		{name: "both null", left: nil, right: nil},
		{name: "both empty", left: "", right: ""},
		{name: "invalid left", left: "not-a-timestamp", right: valid},
		{name: "invalid right", left: valid, right: "not-a-timestamp"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, operator := range []string{"<", "<=", ">", ">=", "="} {
				expression := ComparisonSQL("left_at", "?", operator)
				query := `WITH timestamps(left_at) AS (VALUES (?)) SELECT ` + expression + ` FROM timestamps`
				if got, want := strings.Count(query, "?"), 2; got != want {
					t.Fatalf("comparison query has %d placeholders, want %d", got, want)
				}
				var got bool
				if err := db.QueryRow(query, tc.left, tc.right).Scan(&got); err != nil {
					t.Fatalf("compare missing timestamps with %q: %v", operator, err)
				}
				if got {
					t.Fatalf("comparison with missing/invalid operand should be false: left=%#v operator=%q right=%#v", tc.left, operator, tc.right)
				}
			}
		})
	}
}

func timestampOrderQuery(values []any, descending bool) string {
	selects := make([]string, len(values))
	for index := range values {
		if index == 0 {
			selects[index] = "SELECT ? AS timestamp"
		} else {
			selects[index] = "SELECT ?"
		}
	}
	return `SELECT timestamp FROM (` + strings.Join(selects, " UNION ALL ") + `) ORDER BY ` + OrderBySQL("timestamp", descending)
}

func stringValues(values []string) []any {
	args := make([]any, len(values))
	for index, value := range values {
		args[index] = value
	}
	return args
}

func compareTimes(left, right time.Time, operator string) bool {
	comparison := left.Compare(right)
	switch operator {
	case "<":
		return comparison < 0
	case "<=":
		return comparison <= 0
	case ">":
		return comparison > 0
	case ">=":
		return comparison >= 0
	case "=":
		return comparison == 0
	default:
		panic("unsupported test comparison operator: " + operator)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalNullableStrings(left, right []sql.NullString) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
