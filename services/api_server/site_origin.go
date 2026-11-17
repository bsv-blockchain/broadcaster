package api_server

import (
	"bytes"
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// The export's RSC (Flight) payloads, in the .txt files and inlined into each
// page as self.__next_f.push([1,"…"]) scripts, carry long strings as length-
// prefixed text rows ("3d:T1039,<4153 bytes>"). A plain ReplaceAll of
// originPlaceholder changes those bodies without their lengths, the client
// reads past the end of the row, the rows after it are lost and React throws
// "Connection closed" (minified error #412). So the payloads are rewritten
// row by row and the lengths recomputed.

// nextFlightPushRE matches one inlined Flight chunk. Its argument is JSON: the
// string is escaped with < for '<', so it cannot contain "</script>".
var nextFlightPushRE = regexp.MustCompile(`<script>self\.__next_f\.push\((\[.*?\])\)</script>`)

// flightEdit replaces payload[start:end] with repl.
type flightEdit struct {
	start, end int
	repl       []byte
}

// rewriteOrigin replaces originPlaceholder with origin in the site file name.
func rewriteOrigin(name string, data []byte, origin string) []byte {
	old, repl := []byte(originPlaceholder), []byte(origin)
	if !bytes.Contains(data, old) {
		return data
	}
	switch {
	case strings.HasSuffix(name, ".html"):
		return rewriteHTMLOrigin(data, old, repl)
	case strings.HasSuffix(name, ".txt"):
		// RSC payloads, but also llms.txt and robots.txt: those do not parse
		// as Flight rows and take the plain replace.
		if edits, ok := flightOriginEdits(data, old, repl); ok {
			return applyEdits(data, edits)
		}
	}
	return bytes.ReplaceAll(data, old, repl)
}

// rewriteHTMLOrigin rewrites the Flight payload inlined in a page as a whole
// (a text row can straddle two push scripts), keeps each push script's share of
// it, and plain-replaces the rest of the document.
func rewriteHTMLOrigin(data, old, repl []byte) []byte {
	type chunk struct {
		loc  []int // the push script in data
		from int   // its offset in the joined payload
	}
	var (
		chunks  []chunk
		payload []byte
	)
	for _, loc := range nextFlightPushRE.FindAllSubmatchIndex(data, -1) {
		var args []json.RawMessage
		var s string
		if json.Unmarshal(data[loc[2]:loc[3]], &args) != nil || len(args) != 2 ||
			string(args[0]) != "1" || json.Unmarshal(args[1], &s) != nil {
			continue // [0], [2,…] form state and [3,…] binary chunks carry no rows
		}
		chunks = append(chunks, chunk{loc: loc, from: len(payload)})
		payload = append(payload, s...)
	}
	edits, ok := flightOriginEdits(payload, old, repl)
	if len(chunks) == 0 || !ok {
		return bytes.ReplaceAll(data, old, repl)
	}

	var out bytes.Buffer
	prev := 0
	for i, c := range chunks {
		to := len(payload)
		if i+1 < len(chunks) {
			to = chunks[i+1].from
		}
		out.Write(bytes.ReplaceAll(data[prev:c.loc[0]], old, repl))
		// json.Marshal escapes <, >, & and U+2028/9 as Next does, so the
		// string stays inside its script element.
		s, err := json.Marshal(string(applyEditsRange(payload, edits, c.from, to)))
		if err != nil {
			return bytes.ReplaceAll(data, old, repl)
		}
		out.WriteString("<script>self.__next_f.push([1,")
		out.Write(s)
		out.WriteString("])</script>")
		prev = c.loc[1]
	}
	out.Write(bytes.ReplaceAll(data[prev:], old, repl))
	return out.Bytes()
}

// flightOriginEdits walks a Flight payload row by row and returns the edits
// that replace old with repl in it, recomputing the length of each text row.
// false when payload is not a well-formed sequence of rows.
func flightOriginEdits(payload, old, repl []byte) ([]flightEdit, bool) {
	var edits []flightEdit
	replaceIn := func(from, to int) int {
		n := 0
		for i := from; ; {
			j := bytes.Index(payload[i:to], old)
			if j < 0 {
				return n
			}
			edits = append(edits, flightEdit{start: i + j, end: i + j + len(old), repl: repl})
			i += j + len(old)
			n++
		}
	}
	// rewriteText replaces in the text row body payload[comma+1:end] and, when
	// that changed it, the hex length at payload[hex:comma].
	rewriteText := func(hex, comma, end int) {
		if k := replaceIn(comma+1, end); k > 0 {
			grown := end - comma - 1 + k*(len(repl)-len(old))
			edits = append(edits, flightEdit{start: hex, end: comma, repl: []byte(strconv.FormatInt(int64(grown), 16))})
		}
	}

	for i := 0; i < len(payload); {
		// Row ID: hex digits, then ':'. Hint rows (":HL[…]") have none.
		colon := i
		for colon < len(payload) && isLowerHex(payload[colon]) {
			colon++
		}
		if colon == len(payload) || payload[colon] != ':' {
			return nil, false
		}
		tag := colon + 1
		if tag < len(payload) && isFlightLengthTag(payload[tag]) {
			comma, end, ok := flightRowLength(payload, tag)
			if !ok {
				return nil, false
			}
			// Only text rows hold strings; the others are binary.
			if payload[tag] == 'T' {
				rewriteText(tag+1, comma, end)
			}
			i = end
			continue
		}
		// Every other row runs to its newline.
		end := bytes.IndexByte(payload[tag:], '\n')
		if end < 0 {
			end = len(payload)
		} else {
			end += tag + 1
		}
		replaceIn(tag, end)
		i = end
	}
	sort.Slice(edits, func(a, b int) bool { return edits[a].start < edits[b].start })
	return edits, true
}

// flightRowLength reads the "<hex length>," after a length tag at
// payload[tag] and returns the comma's offset and the end of the row.
func flightRowLength(payload []byte, tag int) (comma, end int, ok bool) {
	comma = tag + 1
	for comma < len(payload) && isLowerHex(payload[comma]) {
		comma++
	}
	if comma == tag+1 || comma == len(payload) || payload[comma] != ',' {
		return 0, 0, false
	}
	n, err := strconv.ParseUint(string(payload[tag+1:comma]), 16, 31)
	end = comma + 1 + int(n)
	if err != nil || end > len(payload) {
		return 0, 0, false
	}
	return comma, end, true
}

// isFlightLengthTag reports whether a row tag is followed by a byte length, as
// React's Flight client reads them (processBinaryChunk).
func isFlightLengthTag(c byte) bool {
	return strings.IndexByte("TAOobUSsLlGgMmV", c) >= 0
}

func isLowerHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f'
}

func applyEdits(payload []byte, edits []flightEdit) []byte {
	return applyEditsRange(payload, edits, 0, len(payload))
}

// applyEditsRange returns payload[from:to] with edits applied. An edit belongs
// to the range holding its start; edits never straddle a row boundary but may
// straddle a chunk boundary, and then the whole replacement goes to the chunk
// it starts in.
func applyEditsRange(payload []byte, edits []flightEdit, from, to int) []byte {
	var out []byte
	pos := from
	for _, e := range edits {
		if e.end <= from || e.start >= to {
			continue
		}
		if e.start < from {
			pos = e.end // written by the previous chunk
			continue
		}
		out = append(out, payload[pos:e.start]...)
		out = append(out, e.repl...)
		pos = e.end
	}
	if pos < to {
		out = append(out, payload[pos:to]...)
	}
	return out
}
