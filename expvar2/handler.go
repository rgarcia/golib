package expvar2

import (
	"bytes"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// ExpvarHandler can serve via HTTP expvar variables as well as custom variables inside
// Exported
type ExpvarHandler struct {
	Exported map[string]expvar.Var
}

// Init the ExpvarHandler, creating any datastructures needed
func (e *ExpvarHandler) Init() {
	e.Exported = make(map[string]expvar.Var)
}

// EnviromentalVariables returns an expvar that also shows env variables
func EnviromentalVariables() expvar.Var {
	return enviromentalVariables(os.Environ)
}

func enviromentalVariables(osEnviron func() []string) expvar.Var {
	return expvar.Func(func() interface{} {
		ret := make(map[string]string)
		for _, env := range osEnviron() {
			parts := strings.SplitN(env, "=", 2)
			if len(parts) <= 1 {
				continue
			}
			ret[parts[0]] = parts[1]
		}
		return ret
	})
}

var _ http.Handler = &ExpvarHandler{}

type filterSet map[string]struct{}

func (s filterSet) shouldFilter(search string) bool {
	if len(s) == 0 {
		return false
	}
	_, exists := s[search]
	return !exists
}

func (e *ExpvarHandler) initialDump(w io.Writer, onlyFetch filterSet) {
	fmt.Fprintf(w, "{")
	first := true
	usedKeys := map[string]struct{}{}
	f := func(kv expvar.KeyValue) {
		if _, exists := usedKeys[kv.Key]; exists {
			return
		}
		if onlyFetch.shouldFilter(kv.Key) {
			return
		}
		if !first {
			fmt.Fprintf(w, ",")
		}
		first = false
		fmt.Fprintf(w, "%q:%s", kv.Key, kv.Value)
	}
	for k, v := range e.Exported {
		f(expvar.KeyValue{
			Key:   k,
			Value: v,
		})
		usedKeys[k] = struct{}{}
	}
	expvar.Do(f)
	fmt.Fprintf(w, "}")
}

func asSet(items []string) filterSet {
	r := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item != "" {
			r[item] = struct{}{}
		}
	}
	return r
}

// ServeHTTP is a copy/past of the private expvar.expvarHandler that I sometimes want to
// register to my own handler.
func (e *ExpvarHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	prettyPrint, _ := strconv.ParseBool(r.URL.Query().Get("pretty"))
	onlyFetch := asSet(strings.Split(r.URL.Query().Get("filter"), ","))
	if prettyPrint {
		tmp := &bytes.Buffer{}
		e.initialDump(tmp, onlyFetch)
		buf := &bytes.Buffer{}
		json.Indent(buf, tmp.Bytes(), "", "\t")
		w.Header().Set("Content-Length", strconv.FormatInt(int64(buf.Len()), 10))
		w.Write(buf.Bytes())
		return
	}
	e.initialDump(w, onlyFetch)
}
