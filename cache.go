package xbase

import (
	"reflect"
	"sort"
	"sync"
)

type fieldDescription struct {
	name     string
	baseType reflect.Type
	typ      reflect.Type
	tag      tag
	index    []int
}

type fieldDescriptions []fieldDescription

func (fs fieldDescriptions) Len() int { return len(fs) }

func (fs fieldDescriptions) Swap(i, j int) { fs[i], fs[j] = fs[j], fs[i] }

func (fs fieldDescriptions) Less(i, j int) bool {
	for k, n := range fs[i].index {
		if n != fs[j].index[k] {
			return n < fs[j].index[k]
		}
	}
	return len(fs[i].index) < len(fs[j].index)
}

type typeKey struct {
	tag string
	reflect.Type
}

type fieldMap map[string]fieldDescriptions

func (m fieldMap) insert(f fieldDescription) {
	fs, ok := m[f.name]
	if !ok {
		m[f.name] = append(fs, f)
		return
	}

	// insert only fields with the shortest path.
	if len(fs[0].index) != len(f.index) {
		return
	}

	// fields that are tagged have priority.
	if !f.tag.empty {
		m[f.name] = append([]fieldDescription{f}, fs...)
		return
	}

	m[f.name] = append(fs, f)
}

func (m fieldMap) fields() fieldDescriptions {
	out := make(fieldDescriptions, 0, len(m))
	for _, v := range m {
		for i, f := range v {
			if f.tag.empty != v[0].tag.empty {
				v = v[:i]
				break
			}
		}
		if len(v) > 1 {
			continue
		}
		out = append(out, v[0])
	}
	sort.Sort(out)
	return out
}

func buildFields(k typeKey) fieldDescriptions {
	type key struct {
		reflect.Type
		tag
	}

	q := fieldDescriptions{{typ: k.Type}}
	visited := make(map[key]struct{})
	fm := make(fieldMap)

	for len(q) > 0 {
		f := q[0]
		q = q[1:]

		key := key{f.typ, f.tag}
		if _, ok := visited[key]; ok {
			continue
		}
		visited[key] = struct{}{}

		depth := len(f.index)

		numField := f.typ.NumField()
		for i := 0; i < numField; i++ {
			sf := f.typ.Field(i)

			if sf.PkgPath != "" && !sf.Anonymous {
				// unexported field
				continue
			}

			if sf.Anonymous {
				t := sf.Type
				if t.Kind() == reflect.Ptr {
					t = t.Elem()
				}
				if sf.PkgPath != "" && t.Kind() != reflect.Struct {
					// ignore embedded unexported non-struct fields.
					continue
				}
			}

			tag := parseTag(k.tag, sf)
			if tag.ignore {
				continue
			}
			if f.tag.prefix != "" {
				tag.prefix += f.tag.prefix
			}

			ft := sf.Type
			if ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
			}

			newf := fieldDescription{
				name:     tag.prefix + tag.name,
				baseType: sf.Type,
				typ:      ft,
				tag:      tag,
				index:    makeIndex(f.index, i),
			}

			if sf.Anonymous && ft.Kind() == reflect.Struct && tag.empty {
				q = append(q, newf)
				continue
			}

			if tag.inline && ft.Kind() == reflect.Struct {
				q = append(q, newf)
				continue
			}

			fm.insert(newf)

			// look for duplicate nodes on the same level. Nodes won't be
			// revisited, so write all fields for the current type now.
			for _, v := range q {
				if len(v.index) != depth {
					break
				}
				if v.typ == f.typ && v.tag.prefix == tag.prefix {
					// other nodes can have different path.
					fm.insert(fieldDescription{
						name:     tag.prefix + tag.name,
						baseType: sf.Type,
						typ:      ft,
						tag:      tag,
						index:    makeIndex(v.index, i),
					})
				}
			}
		}
	}
	return fm.fields()
}

func makeIndex(index []int, v int) []int {
	out := make([]int, len(index), len(index)+1)
	copy(out, index)
	return append(out, v)
}

var fieldCache = struct {
	mtx sync.RWMutex
	m   map[typeKey][]fieldDescription
}{m: make(map[typeKey][]fieldDescription)}

func cachedFields(k typeKey) fieldDescriptions {
	fieldCache.mtx.RLock()
	fields, ok := fieldCache.m[k]
	fieldCache.mtx.RUnlock()

	if ok {
		return fields
	}

	fields = buildFields(k)

	fieldCache.mtx.Lock()
	fieldCache.m[k] = fields
	fieldCache.mtx.Unlock()

	return fields
}
