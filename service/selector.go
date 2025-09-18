package service

import (
	"bytes"
	"math"

	"github.com/mattgonewild/chasm/core"
	"github.com/mattgonewild/chasm/service/proto"
)

const (
	hasCode uint = 1 << iota
	hasSince
	hasTag
	hasConfig
	hasReport
	matchTableCap
)

type matchFunc = func(*selector, core.DaemonInfo) bool

var (
	matchTable [matchTableCap]matchFunc
)

type selector struct {
	filter matchFunc
	bitmap uint
	code   int
	since  int64
	tag    string
	config []byte
	report []byte
}

func (this *selector) Select(daemon core.DaemonInfo) bool { return this.filter(this, daemon) }

func (this *selector) LoadFilter(filter *proto.Filter) {
	this.bitmap = 0

	if filter.Status != nil {
		if filter.Status.Code > math.MinInt64 {
			this.bitmap |= hasCode
			this.code = int(filter.Status.Code)
		}

		if filter.Status.Since > math.MinInt64 {
			this.bitmap |= hasSince
			this.since = filter.Status.Since
		}
	}

	if filter.Tag != nil {
		this.bitmap |= hasTag
		this.tag = *filter.Tag
	}

	if filter.Config != nil {
		this.bitmap |= hasConfig
		this.config = filter.Config
	}

	if filter.Report != nil {
		this.bitmap |= hasReport
		this.report = filter.Report
	}

	this.filter = matchTable[this.bitmap]
}

func init() {
	for index := range matchTable {
		matchTable[index] = getMatchFunc(uint(index))
	}
}

func getMatchFunc(bitmap uint) matchFunc {
	switch bitmap {
	case 0:
		return matchAll
	case hasCode:
		return matchCode
	case hasSince:
		return matchSince
	case hasCode | hasSince:
		return matchCodeSince
	case hasTag:
		return matchTag
	case hasCode | hasTag:
		return matchCodeTag
	case hasSince | hasTag:
		return matchSinceTag
	case hasCode | hasSince | hasTag:
		return matchCodeSinceTag
	case hasConfig:
		return matchConfig
	case hasCode | hasConfig:
		return matchCodeConfig
	case hasSince | hasConfig:
		return matchSinceConfig
	case hasCode | hasSince | hasConfig:
		return matchCodeSinceConfig
	case hasTag | hasConfig:
		return matchTagConfig
	case hasCode | hasTag | hasConfig:
		return matchCodeTagConfig
	case hasSince | hasTag | hasConfig:
		return matchSinceTagConfig
	case hasCode | hasSince | hasTag | hasConfig:
		return matchCodeSinceTagConfig
	case hasReport:
		return matchReport
	case hasCode | hasReport:
		return matchCodeReport
	case hasSince | hasReport:
		return matchSinceReport
	case hasCode | hasSince | hasReport:
		return matchCodeSinceReport
	case hasTag | hasReport:
		return matchTagReport
	case hasCode | hasTag | hasReport:
		return matchCodeTagReport
	case hasSince | hasTag | hasReport:
		return matchSinceTagReport
	case hasCode | hasSince | hasTag | hasReport:
		return matchCodeSinceTagReport
	case hasConfig | hasReport:
		return matchConfigReport
	case hasCode | hasConfig | hasReport:
		return matchCodeConfigReport
	case hasSince | hasConfig | hasReport:
		return matchSinceConfigReport
	case hasCode | hasSince | hasConfig | hasReport:
		return matchCodeSinceConfigReport
	case hasTag | hasConfig | hasReport:
		return matchTagConfigReport
	case hasCode | hasTag | hasConfig | hasReport:
		return matchCodeTagConfigReport
	case hasSince | hasTag | hasConfig | hasReport:
		return matchSinceTagConfigReport
	case hasCode | hasSince | hasTag | hasConfig | hasReport:
		return matchCodeSinceTagConfigReport
	default:
		return matchAll
	}
}

func matchAll(_ *selector, _ core.DaemonInfo) bool { return true }

func matchCode(fi *selector, di core.DaemonInfo) bool {
	code, _ := di.Status()
	return fi.code == code
}

func matchSince(fi *selector, di core.DaemonInfo) bool {
	_, since := di.Status()
	return since >= fi.since
}

func matchCodeSince(fi *selector, di core.DaemonInfo) bool {
	code, since := di.Status()
	return fi.code == code && since >= fi.since
}

func matchTag(fi *selector, di core.DaemonInfo) bool { return fi.tag == di.Tag() }

func matchCodeTag(fi *selector, di core.DaemonInfo) bool {
	code, _ := di.Status()
	return fi.code == code && fi.tag == di.Tag()
}

func matchSinceTag(fi *selector, di core.DaemonInfo) bool {
	_, since := di.Status()
	return since >= fi.since && fi.tag == di.Tag()
}

func matchCodeSinceTag(fi *selector, di core.DaemonInfo) bool {
	code, since := di.Status()
	return fi.code == code && since >= fi.since && fi.tag == di.Tag()
}

func matchConfig(fi *selector, di core.DaemonInfo) bool {
	return bytes.Contains(di.Config(), fi.config)
}

func matchCodeConfig(fi *selector, di core.DaemonInfo) bool {
	code, _ := di.Status()
	return fi.code == code && bytes.Contains(di.Config(), fi.config)
}

func matchSinceConfig(fi *selector, di core.DaemonInfo) bool {
	_, since := di.Status()
	return since >= fi.since && bytes.Contains(di.Config(), fi.config)
}

func matchCodeSinceConfig(fi *selector, di core.DaemonInfo) bool {
	code, since := di.Status()
	return fi.code == code && since >= fi.since && bytes.Contains(di.Config(), fi.config)
}

func matchTagConfig(fi *selector, di core.DaemonInfo) bool {
	return fi.tag == di.Tag() && bytes.Contains(di.Config(), fi.config)
}

func matchCodeTagConfig(fi *selector, di core.DaemonInfo) bool {
	code, _ := di.Status()
	return fi.code == code && fi.tag == di.Tag() && bytes.Contains(di.Config(), fi.config)
}

func matchSinceTagConfig(fi *selector, di core.DaemonInfo) bool {
	_, since := di.Status()
	return since >= fi.since && fi.tag == di.Tag() && bytes.Contains(di.Config(), fi.config)
}

func matchCodeSinceTagConfig(fi *selector, di core.DaemonInfo) bool {
	code, since := di.Status()
	return fi.code == code && since >= fi.since && fi.tag == di.Tag() && bytes.Contains(di.Config(), fi.config)
}

func matchReport(fi *selector, di core.DaemonInfo) bool {
	return bytes.Contains(di.Report(), fi.report)
}

func matchCodeReport(fi *selector, di core.DaemonInfo) bool {
	code, _ := di.Status()
	return fi.code == code && bytes.Contains(di.Report(), fi.report)
}

func matchSinceReport(fi *selector, di core.DaemonInfo) bool {
	_, since := di.Status()
	return since >= fi.since && bytes.Contains(di.Report(), fi.report)
}

func matchCodeSinceReport(fi *selector, di core.DaemonInfo) bool {
	code, since := di.Status()
	return fi.code == code && since >= fi.since && bytes.Contains(di.Report(), fi.report)
}

func matchTagReport(fi *selector, di core.DaemonInfo) bool {
	return fi.tag == di.Tag() && bytes.Contains(di.Report(), fi.report)
}

func matchCodeTagReport(fi *selector, di core.DaemonInfo) bool {
	code, _ := di.Status()
	return fi.code == code && fi.tag == di.Tag() && bytes.Contains(di.Report(), fi.report)
}

func matchSinceTagReport(fi *selector, di core.DaemonInfo) bool {
	_, since := di.Status()
	return since >= fi.since && fi.tag == di.Tag() && bytes.Contains(di.Report(), fi.report)
}

func matchCodeSinceTagReport(fi *selector, di core.DaemonInfo) bool {
	code, since := di.Status()
	return fi.code == code && since >= fi.since && fi.tag == di.Tag() && bytes.Contains(di.Report(), fi.report)
}

func matchConfigReport(fi *selector, di core.DaemonInfo) bool {
	return bytes.Contains(di.Config(), fi.config) && bytes.Contains(di.Report(), fi.report)
}

func matchCodeConfigReport(fi *selector, di core.DaemonInfo) bool {
	code, _ := di.Status()
	return fi.code == code && bytes.Contains(di.Config(), fi.config) && bytes.Contains(di.Report(), fi.report)
}

func matchSinceConfigReport(fi *selector, di core.DaemonInfo) bool {
	_, since := di.Status()
	return since >= fi.since && bytes.Contains(di.Config(), fi.config) && bytes.Contains(di.Report(), fi.report)
}

func matchCodeSinceConfigReport(fi *selector, di core.DaemonInfo) bool {
	code, since := di.Status()
	return fi.code == code && since >= fi.since && bytes.Contains(di.Config(), fi.config) && bytes.Contains(di.Report(), fi.report)
}

func matchTagConfigReport(fi *selector, di core.DaemonInfo) bool {
	return fi.tag == di.Tag() && bytes.Contains(di.Config(), fi.config) && bytes.Contains(di.Report(), fi.report)
}

func matchCodeTagConfigReport(fi *selector, di core.DaemonInfo) bool {
	code, _ := di.Status()
	return fi.code == code && fi.tag == di.Tag() && bytes.Contains(di.Config(), fi.config) && bytes.Contains(di.Report(), fi.report)
}

func matchSinceTagConfigReport(fi *selector, di core.DaemonInfo) bool {
	_, since := di.Status()
	return since >= fi.since && fi.tag == di.Tag() && bytes.Contains(di.Config(), fi.config) && bytes.Contains(di.Report(), fi.report)
}

func matchCodeSinceTagConfigReport(fi *selector, di core.DaemonInfo) bool {
	code, since := di.Status()
	return fi.code == code && since >= fi.since && fi.tag == di.Tag() && bytes.Contains(di.Config(), fi.config) && bytes.Contains(di.Report(), fi.report)
}
