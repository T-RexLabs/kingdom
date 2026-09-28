// Package timestamporder builds SQLite expressions that preserve the full
// ordering of RFC3339Nano timestamp strings, including offsets and nanoseconds.
package timestamporder

import "strings"

// OrderBySQL returns SQL ordering terms that compare RFC3339Nano instants at
// nanosecond precision. The timestamp expression is expected to contain a
// valid RFC3339Nano string or NULL.
func OrderBySQL(expression string, descending bool) string {
	direction := "ASC"
	if descending {
		direction = "DESC"
	}
	seconds := secondsSQL(expression)
	fraction := fractionalNanosecondsSQL(expression)
	return strings.Join([]string{
		seconds + " " + direction,
		fraction + " " + direction,
		"(" + expression + ") " + direction,
	}, ", ")
}

// ComparisonSQL returns an exact RFC3339Nano comparison for <, <=, >, >=, or
// =. Null, empty, and malformed timestamps do not satisfy a comparison.
// rightExpression appears once in a scalar subquery, so a bound parameter can
// be reused without reducing its precision or duplicating its argument.
func ComparisonSQL(leftExpression, rightExpression, operator string) string {
	leftSeconds := secondsSQL(leftExpression)
	leftFraction := fractionalNanosecondsSQL(leftExpression)
	rightSeconds := secondsSQL("timestamporder_rhs.value")
	rightFraction := fractionalNanosecondsSQL("timestamporder_rhs.value")

	var comparison string
	switch operator {
	case "<":
		comparison = orderedPairComparison(leftSeconds, leftFraction, rightSeconds, rightFraction, "<")
	case "<=":
		comparison = orderedPairComparison(leftSeconds, leftFraction, rightSeconds, rightFraction, "<=")
	case ">":
		comparison = orderedPairComparison(leftSeconds, leftFraction, rightSeconds, rightFraction, ">")
	case ">=":
		comparison = orderedPairComparison(leftSeconds, leftFraction, rightSeconds, rightFraction, ">=")
	case "=":
		comparison = orderedPairComparison(leftSeconds, leftFraction, rightSeconds, rightFraction, "=")
	default:
		panic("unsupported RFC3339Nano comparison operator: " + operator)
	}

	return "EXISTS (SELECT 1 FROM (SELECT " + rightExpression + " AS value) AS timestamporder_rhs WHERE " + comparison + ")"
}

func orderedPairComparison(leftSeconds, leftFraction, rightSeconds, rightFraction, operator string) string {
	return "(" + leftSeconds + ", " + leftFraction + ") " + operator + " (" + rightSeconds + ", " + rightFraction + ")"
}

func secondsSQL(expression string) string {
	value := "(" + expression + ")"
	year := "CAST(substr(" + value + ", 1, 4) AS INTEGER)"
	month := "CAST(substr(" + value + ", 6, 2) AS INTEGER)"
	day := "CAST(substr(" + value + ", 9, 2) AS INTEGER)"
	hour := "CAST(substr(" + value + ", 12, 2) AS INTEGER)"
	minute := "CAST(substr(" + value + ", 15, 2) AS INTEGER)"
	second := "CAST(substr(" + value + ", 18, 2) AS INTEGER)"

	// Count Gregorian days from the start of year 0000, then subtract the
	// numeric UTC offset. Integer arithmetic avoids SQLite date-function range
	// boundaries and keeps the separate nanosecond key exact.
	daysBeforeYear := "(" + year + " * 365 + (" + year + " + 3) / 4 - (" + year + " + 99) / 100 + (" + year + " + 399) / 400)"
	daysBeforeMonth := "(CASE " + month + " WHEN 1 THEN 0 WHEN 2 THEN 31 WHEN 3 THEN 59 WHEN 4 THEN 90 WHEN 5 THEN 120 WHEN 6 THEN 151 WHEN 7 THEN 181 WHEN 8 THEN 212 WHEN 9 THEN 243 WHEN 10 THEN 273 WHEN 11 THEN 304 WHEN 12 THEN 334 ELSE 0 END)"
	leapYear := "(" + year + " % 4 = 0 AND (" + year + " % 100 <> 0 OR " + year + " % 400 = 0))"
	leapDay := "(CASE WHEN " + month + " > 2 AND " + leapYear + " THEN 1 ELSE 0 END)"
	localSeconds := "((" + daysBeforeYear + " + " + daysBeforeMonth + " + " + leapDay + " + " + day + " - 1) * 86400 + " + hour + " * 3600 + " + minute + " * 60 + " + second + " - " + utcOffsetSecondsSQL(value) + ")"
	return "CASE WHEN " + validRFC3339NanoSQL(value) + " THEN " + localSeconds + " ELSE NULL END"
}

func utcOffsetSecondsSQL(value string) string {
	zoneOffset := "(CAST(substr(" + value + ", -5, 2) AS INTEGER) * 3600 + CAST(substr(" + value + ", -2, 2) AS INTEGER) * 60)"
	signedOffset := "(CASE WHEN substr(" + value + ", -6, 1) = '-' THEN -1 ELSE 1 END * " + zoneOffset + ")"
	return "CASE WHEN substr(" + value + ", -1, 1) = 'Z' THEN 0 ELSE " + signedOffset + " END"
}

func validRFC3339NanoSQL(value string) string {
	substr := func(start, count string) string {
		return "substr(" + value + ", " + start + ", " + count + ")"
	}
	length := "length(" + value + ")"
	integer := func(expression string) string {
		return "CAST(" + expression + " AS INTEGER)"
	}
	year := integer(substr("1", "4"))
	month := integer(substr("6", "2"))
	day := integer(substr("9", "2"))
	hour := integer(substr("12", "2"))
	minute := integer(substr("15", "2"))
	second := integer(substr("18", "2"))
	leapYear := "(" + year + " % 4 = 0 AND (" + year + " % 100 <> 0 OR " + year + " % 400 = 0))"
	daysInMonth := "(CASE " + month + " WHEN 1 THEN 31 WHEN 2 THEN CASE WHEN " + leapYear + " THEN 29 ELSE 28 END WHEN 3 THEN 31 WHEN 4 THEN 30 WHEN 5 THEN 31 WHEN 6 THEN 30 WHEN 7 THEN 31 WHEN 8 THEN 31 WHEN 9 THEN 30 WHEN 10 THEN 31 WHEN 11 THEN 30 WHEN 12 THEN 31 ELSE 0 END)"
	digitOnly := func(expression string) string {
		return "(" + expression + " <> '' AND " + expression + " NOT GLOB '*[^0-9]*')"
	}
	fraction := substr("21", length+" - 21")
	offsetFraction := substr("21", length+" - 26")
	validUTC := "(" + strings.Join([]string{
		substr("-1", "1") + " = 'Z'",
		"(" + length + " = 20 OR (" + length + " BETWEEN 22 AND 30 AND " + substr("20", "1") + " = '.' AND " + digitOnly(fraction) + "))",
	}, " AND ") + ")"
	validOffset := "(" + strings.Join([]string{
		substr("-6", "1") + " IN ('+', '-')",
		substr("-3", "1") + " = ':'",
		digitOnly(substr("-5", "2")),
		digitOnly(substr("-2", "2")),
		integer(substr("-5", "2")) + " BETWEEN 0 AND 23",
		integer(substr("-2", "2")) + " BETWEEN 0 AND 59",
		"(" + length + " = 25 OR (" + length + " BETWEEN 27 AND 35 AND " + substr("20", "1") + " = '.' AND " + digitOnly(offsetFraction) + "))",
	}, " AND ") + ")"
	return "(" + strings.Join([]string{
		value + " IS NOT NULL",
		value + " = TRIM(" + value + ")",
		"length(CAST(" + value + " AS BLOB)) = " + length,
		"(" + length + " BETWEEN 20 AND 35)",
		substr("5", "1") + " = '-'",
		substr("8", "1") + " = '-'",
		substr("11", "1") + " = 'T'",
		substr("14", "1") + " = ':'",
		substr("17", "1") + " = ':'",
		digitOnly(substr("1", "4")),
		digitOnly(substr("6", "2")),
		digitOnly(substr("9", "2")),
		digitOnly(substr("12", "2")),
		digitOnly(substr("15", "2")),
		digitOnly(substr("18", "2")),
		"(" + month + " BETWEEN 1 AND 12)",
		"(" + day + " BETWEEN 1 AND " + daysInMonth + ")",
		"(" + hour + " BETWEEN 0 AND 23)",
		"(" + minute + " BETWEEN 0 AND 59)",
		"(" + second + " BETWEEN 0 AND 59)",
		"(" + validUTC + " OR " + validOffset + ")",
	}, " AND ") + ")"
}

func fractionalNanosecondsSQL(expression string) string {
	value := "(" + expression + ")"
	timeZoneLength := "CASE WHEN substr(" + value + ", -1, 1) = 'Z' THEN 1 ELSE 6 END"
	fraction := "substr(" + value + ", 21, length(" + value + ") - 20 - " + timeZoneLength + ")"
	return "CASE WHEN substr(" + value + ", 20, 1) = '.' THEN substr(" + fraction + " || '000000000', 1, 9) ELSE '000000000' END"
}
