/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package bolt

import ()

// Called by packstream unpacker to hydrate a packstream struct into something
// more usable by the consumer.
/*
func hydrate(tag packstream.StructTag, fields []interface{}) (interface{}, error) {
	switch tag {
	case msgSuccess:
		return hydrateSuccess(fields)
	case msgIgnored:
		return hydrateIgnored(fields)
	case msgFailure:
		return hydrateFailure(fields)
	case msgRecord:
		return hydrateRecord(fields)
	case 'N':
		return hydrateNode(fields)
	case 'R':
		return hydrateRelationship(fields)
	case 'r':
		return hydrateRelNode(fields)
	case 'P':
		return hydratePath(fields)
	case 'X':
		return hydratePoint2d(fields)
	case 'Y':
		return hydratePoint3d(fields)
	case 'F':
		return hydrateDateTimeOffset(fields)
	case 'f':
		return hydrateDateTimeNamedZone(fields)
	case 'd':
		return hydrateLocalDateTime(fields)
	case 'D':
		return hydrateDate(fields)
	case 'T':
		return hydrateTime(fields)
	case 't':
		return hydrateLocalTime(fields)
	case 'E':
		return hydrateDuration(fields)
	default:
		return nil, errors.New(fmt.Sprintf("Unknown tag: %02x", tag))
	}
}

func hydrateNode(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("Node hydrate error")
	}
	id, idok := fields[0].(int64)
	tagsx, tagsok := fields[1].([]interface{})
	props, propsok := fields[2].(map[string]interface{})
	if !idok || !tagsok || !propsok {
		return nil, errors.New("Node hydrate error")
	}
	n := dbtype.Node{Id: id, Props: props, Labels: make([]string, len(tagsx))}
	// Convert tags to strings
	for i, x := range tagsx {
		t, tok := x.(string)
		if !tok {
			return nil, errors.New("Node hydrate error")
		}
		n.Labels[i] = t
	}
	return n, nil
}

func hydrateRelationship(fields []interface{}) (interface{}, error) {
	if len(fields) != 5 {
		return nil, errors.New("Relationship hydrate error")
	}
	id, idok := fields[0].(int64)
	sid, sidok := fields[1].(int64)
	eid, eidok := fields[2].(int64)
	lbl, lblok := fields[3].(string)
	props, propsok := fields[4].(map[string]interface{})
	if !idok || !sidok || !eidok || !lblok || !propsok {
		return nil, errors.New("Relationship hydrate error")
	}
	return dbtype.Relationship{Id: id, StartId: sid, EndId: eid, Type: lbl, Props: props}, nil
}

func hydrateRelNode(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("RelNode hydrate error")
	}
	id, idok := fields[0].(int64)
	lbl, lblok := fields[1].(string)
	props, propsok := fields[2].(map[string]interface{})
	if !idok || !lblok || !propsok {
		return nil, errors.New("RelNode hydrate error")
	}
	s := &relNode{id: id, name: lbl, props: props}
	return s, nil
}

func hydratePath(fields []interface{}) (interface{}, error) {
	if len(fields) != 3 {
		return nil, errors.New("Path hydrate error")
	}
	nodesx, nok := fields[0].([]interface{})
	relnodesx, rok := fields[1].([]interface{})
	indsx, iok := fields[2].([]interface{})
	if !nok || !rok || !iok {
		return nil, errors.New("Path hydrate error")
	}

	nodes := make([]dbtype.Node, len(nodesx))
	for i, nx := range nodesx {
		n, ok := nx.(dbtype.Node)
		if !ok {
			return nil, errors.New("Path hydrate error")
		}
		nodes[i] = n
	}

	relnodes := make([]*relNode, len(relnodesx))
	for i, rx := range relnodesx {
		r, ok := rx.(*relNode)
		if !ok {
			return nil, errors.New("Path hydrate error")
		}
		relnodes[i] = r
	}

	indexes := make([]int, len(indsx))
	for i, ix := range indsx {
		p, ok := ix.(int64)
		if !ok {
			return nil, errors.New("Path hydrate error")
		}
		indexes[i] = int(p)
	}
	// Must be even number
	if (len(indexes) & 0x01) == 1 {
		return nil, errors.New("Path hydrate error")
	}

	return buildPath(nodes, relnodes, indexes), nil
}

func hydrateSuccess(fields []interface{}) (interface{}, error) {
	if len(fields) != 1 {
		return nil, errors.New("Success hydrate error")
	}
	meta, metaok := fields[0].(map[string]interface{})
	if !metaok {
		return nil, errors.New("Success hydrate error")
	}
	return &successResponse{meta: meta}, nil
}

func hydrateRecord(fields []interface{}) (interface{}, error) {
	if len(fields) == 0 {
		return nil, errors.New("Record hydrate error")
	}
	v, vok := fields[0].([]interface{})
	if !vok {
		return nil, errors.New("Record hydrate error")
	}
	return &db.Record{Values: v}, nil
}
*/
