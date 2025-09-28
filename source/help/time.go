package help

const (
	start = 2025
	end   = start + 100
)

type beforeTable struct {
	secBeforePoint  [end - start][13]int64
	secBeforeDay    [31]int64
	secBeforeHour   [24]int64
	secBeforeMinute [60]int64
}

func newBeforeTable() beforeTable {
	var (
		dayBeforeYear [end - start]int32
		isLeapYear    [end - start]bool
		day           int32
	)

	for year := start; year < end; year++ {
		off := year - start
		dayBeforeYear[off] = day

		leapYear := (year%4 == 0) && (year%100 != 0 || year%400 == 0)
		isLeapYear[off] = leapYear

		if leapYear {
			day += 366
		} else {
			day += 365
		}
	}

	var (
		dayBeforeNormMonth = [13]int16{0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334}
		dayBeforeLeapMonth = [13]int16{0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335}
		secBeforeYear      [end - start]int64
		secBeforeNormMonth [13]int64
		secBeforeLeapMonth [13]int64

		table beforeTable
	)

	year := len(dayBeforeYear)
	for year := range year {
		secBeforeYear[year] = int64(dayBeforeYear[year]) * 86400
	}

	for month := range len(dayBeforeNormMonth) {
		secBeforeNormMonth[month] = int64(dayBeforeNormMonth[month]) * 86400
		secBeforeLeapMonth[month] = int64(dayBeforeLeapMonth[month]) * 86400
	}

	for year := range year {
		base := secBeforeYear[year]
		if isLeapYear[year] {
			for month := 1; month <= 12; month++ {
				table.secBeforePoint[year][month] = base + secBeforeLeapMonth[month]
			}
		} else {
			for month := 1; month <= 12; month++ {
				table.secBeforePoint[year][month] = base + secBeforeNormMonth[month]
			}
		}
	}

	for day := range len(table.secBeforeDay) {
		table.secBeforeDay[day] = int64(day) * 86400
	}

	for hour := range len(table.secBeforeHour) {
		table.secBeforeHour[hour] = int64(hour) * 3600
	}

	for minute := range len(table.secBeforeMinute) {
		table.secBeforeMinute[minute] = int64(minute) * 60
	}

	return table
}

var table beforeTable = newBeforeTable()

// RFC3339 parses an RFC 3339 timestamp and returns the number of nanoseconds since the Unix epoch.
// Only UTC ('Z') is accepted; no numeric offsets or leap seconds. There is no day-of-month validation.
// Year must be in [2025, 2125). Fractional seconds may have 1–9 digits or be omitted.
func RFC3339(raw []byte) (unixTime int64, ok bool) {
	const (
		unixEpoch int64 = 1735689600
		e9        int64 = 1e9
	)

	length := len(raw)

	if length < 20 || length > 30 || raw[length-1] != 'Z' {
		return
	}

	if raw[4] != '-' || raw[7] != '-' || raw[10] != 'T' || raw[13] != ':' || raw[16] != ':' {
		return
	}

	y0 := digit(raw[0])
	y1 := digit(raw[1])
	y2 := digit(raw[2])
	y3 := digit(raw[3])
	if y0|y1|y2|y3 < 0 {
		return
	}

	year := y0*1000 + y1*100 + y2*10 + y3
	if year < start || year >= end {
		return
	}

	m0 := digit(raw[5])
	m1 := digit(raw[6])
	if m0|m1 < 0 {
		return
	}

	month := m0*10 + m1
	if month < 1 || month > 12 {
		return
	}

	d0 := digit(raw[8])
	d1 := digit(raw[9])
	if d0|d1 < 0 {
		return
	}

	day := d0*10 + d1
	if day < 1 || day > 31 {
		return
	}

	h0 := digit(raw[11])
	h1 := digit(raw[12])
	if h0|h1 < 0 {
		return
	}

	hour := h0*10 + h1
	if hour > 23 {
		return
	}

	i0 := digit(raw[14])
	i1 := digit(raw[15])
	if i0|i1 < 0 {
		return
	}

	minute := i0*10 + i1
	if minute > 59 {
		return
	}

	s0 := digit(raw[17])
	s1 := digit(raw[18])
	if s0|s1 < 0 {
		return
	}

	second := s0*10 + s1
	if second > 59 {
		return
	}

	if length == 20 {
		return (unixEpoch +
			table.secBeforePoint[year-start][month] +
			table.secBeforeDay[day-1] +
			table.secBeforeHour[hour] +
			table.secBeforeMinute[minute] +
			int64(second)) * e9, true
	}

	if raw[19] != '.' {
		return
	}

	fracDigit := length - 21
	if fracDigit < 1 || fracDigit > 9 {
		return
	}
	base := 20

	var fraction int32
	if fracDigit == 9 {
		d0 := digit(raw[base])
		d1 := digit(raw[base+1])
		d2 := digit(raw[base+2])
		d3 := digit(raw[base+3])
		d4 := digit(raw[base+4])
		d5 := digit(raw[base+5])
		d6 := digit(raw[base+6])
		d7 := digit(raw[base+7])
		d8 := digit(raw[base+8])
		if d0|d1|d2|d3|d4|d5|d6|d7|d8 < 0 {
			return
		}

		fraction = int32(d0)*100000000 +
			int32(d1)*10000000 +
			int32(d2)*1000000 +
			int32(d3)*100000 +
			int32(d4)*10000 +
			int32(d5)*1000 +
			int32(d6)*100 +
			int32(d7)*10 +
			int32(d8)
	} else {
		switch fracDigit {
		case 1:
			d0 := digit(raw[base])
			if d0 < 0 {
				return
			}

			fraction = int32(d0) * 100000000
		case 2:
			d0 := digit(raw[base])
			d1 := digit(raw[base+1])
			if d0|d1 < 0 {
				return
			}

			fraction = int32(d0*10+d1) * 10000000
		case 3:
			d0 := digit(raw[base])
			d1 := digit(raw[base+1])
			d2 := digit(raw[base+2])
			if d0|d1|d2 < 0 {
				return
			}

			fraction = int32(d0*100+d1*10+d2) * 1000000
		case 4:
			d0 := digit(raw[base])
			d1 := digit(raw[base+1])
			d2 := digit(raw[base+2])
			d3 := digit(raw[base+3])
			if d0|d1|d2|d3 < 0 {
				return
			}

			fraction = int32(d0*1000+d1*100+d2*10+d3) * 100000
		case 5:
			d0 := digit(raw[base])
			d1 := digit(raw[base+1])
			d2 := digit(raw[base+2])
			d3 := digit(raw[base+3])
			d4 := digit(raw[base+4])
			if d0|d1|d2|d3|d4 < 0 {
				return
			}

			fraction = int32(d0*10000+d1*1000+d2*100+d3*10+d4) * 10000
		case 6:
			d0 := digit(raw[base])
			d1 := digit(raw[base+1])
			d2 := digit(raw[base+2])
			d3 := digit(raw[base+3])
			d4 := digit(raw[base+4])
			d5 := digit(raw[base+5])
			if d0|d1|d2|d3|d4|d5 < 0 {
				return
			}

			fraction = int32(d0*100000+d1*10000+d2*1000+d3*100+d4*10+d5) * 1000
		case 7:
			d0 := digit(raw[base])
			d1 := digit(raw[base+1])
			d2 := digit(raw[base+2])
			d3 := digit(raw[base+3])
			d4 := digit(raw[base+4])
			d5 := digit(raw[base+5])
			d6 := digit(raw[base+6])
			if d0|d1|d2|d3|d4|d5|d6 < 0 {
				return
			}

			fraction = int32(d0*1000000+d1*100000+d2*10000+d3*1000+d4*100+d5*10+d6) * 100
		case 8:
			d0 := digit(raw[base])
			d1 := digit(raw[base+1])
			d2 := digit(raw[base+2])
			d3 := digit(raw[base+3])
			d4 := digit(raw[base+4])
			d5 := digit(raw[base+5])
			d6 := digit(raw[base+6])
			d7 := digit(raw[base+7])
			if d0|d1|d2|d3|d4|d5|d6|d7 < 0 {
				return
			}

			fraction = int32(d0*10000000+d1*1000000+d2*100000+d3*10000+d4*1000+d5*100+d6*10+d7) * 10
		default:
			return
		}
	}

	return (unixEpoch+
		table.secBeforePoint[year-start][month]+
		table.secBeforeDay[day-1]+
		table.secBeforeHour[hour]+
		table.secBeforeMinute[minute]+
		int64(second))*e9 + int64(fraction), true
}

func digit(value byte) int {
	digit := int(value - '0')
	if uint(digit) > 9 {
		return -1
	}

	return digit
}
