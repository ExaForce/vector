package metadata

remap: functions: parse_cef: {
	category: "Parse"
	description: """
		Parses the `value` in CEF (Common Event Format) format. Ignores everything up to CEF header. Empty values are returned as empty strings. Surrounding quotes are removed from values.
		"""
	notices: [
		"""
			All values are returned as strings. We recommend manually coercing values to desired types as you see fit.
			""",
	]

	arguments: [
		{
			name:        "value"
			description: "The string to parse."
			required:    true
			type: ["string"]
		},
		{
			name:        "translate_custom_fields"
			description: "Toggles translation of custom field pairs to `key:value`."
			required:    false
			type: ["boolean"]
		},
	]
	internal_failure_reasons: [
		"`value` is not a properly formatted CEF string.",
	]
	return: types: ["object"]

	examples: [
		{
			title: "Parse output generated by PTA"
			source: #"""
				parse_cef!(
					"CEF:0|CyberArk|PTA|12.6|1|Suspected credentials theft|8|suser=mike2@prod1.domain.com shost=prod1.domain.com src=1.1.1.1 duser=andy@dev1.domain.com dhost=dev1.domain.com dst=2.2.2.2 cs1Label=ExtraData cs1=None cs2Label=EventID cs2=52b06812ec3500ed864c461e deviceCustomDate1Label=detectionDate deviceCustomDate1=1388577900000 cs3Label=PTAlink cs3=https://1.1.1.1/incidents/52b06812ec3500ed864c461e cs4Label=ExternalLink cs4=None"
				)
				"""#
			return: {
				"cefVersion":             "0"
				"deviceVendor":           "CyberArk"
				"deviceProduct":          "PTA"
				"deviceVersion":          "12.6"
				"deviceEventClassId":     "1"
				"name":                   "Suspected credentials theft"
				"severity":               "8"
				"suser":                  "mike2@prod1.domain.com"
				"shost":                  "prod1.domain.com"
				"src":                    "1.1.1.1"
				"duser":                  "andy@dev1.domain.com"
				"dhost":                  "dev1.domain.com"
				"dst":                    "2.2.2.2"
				"cs1Label":               "ExtraData"
				"cs1":                    "None"
				"cs2Label":               "EventID"
				"cs2":                    "52b06812ec3500ed864c461e"
				"deviceCustomDate1Label": "detectionDate"
				"deviceCustomDate1":      "1388577900000"
				"cs3Label":               "PTAlink"
				"cs3":                    "https://1.1.1.1/incidents/52b06812ec3500ed864c461e"
				"cs4Label":               "ExternalLink"
				"cs4":                    "None"
			}
		},
		{
			title: "Ignore syslog header"
			source: #"""
				parse_cef!(
					"Sep 29 08:26:10 host CEF:1|Security|threatmanager|1.0|100|worm successfully stopped|10|src=10.0.0.1 dst=2.1.2.2 spt=1232"
				)
				"""#
			return: {
				"cefVersion":         "1"
				"deviceVendor":       "Security"
				"deviceProduct":      "threatmanager"
				"deviceVersion":      "1.0"
				"deviceEventClassId": "100"
				"name":               "worm successfully stopped"
				"severity":           "10"
				"src":                "10.0.0.1"
				"dst":                "2.1.2.2"
				"spt":                "1232"
			}
		},
		{
			title: "Translate custom fields"
			source: #"""
				parse_cef!(
					"CEF:0|Dev|firewall|2.2|1|Connection denied|5|c6a1=2345:0425:2CA1:0000:0000:0567:5673:23b5 c6a1Label=Device IPv6 Address",
					translate_custom_fields: true
				)
				"""#
			return: {
				"cefVersion":          "0"
				"deviceVendor":        "Dev"
				"deviceProduct":       "firewall"
				"deviceVersion":       "2.2"
				"deviceEventClassId":  "1"
				"name":                "Connection denied"
				"severity":            "5"
				"Device IPv6 Address": "2345:0425:2CA1:0000:0000:0567:5673:23b5"
			}
		},
	]
}
