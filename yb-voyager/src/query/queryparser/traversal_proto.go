/*
Copyright (c) YugabyteDB, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package queryparser

import (
	"fmt"
	"slices"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// function type for processing nodes during traversal
type NodeProcessor func(msg protoreflect.Message) error

/*
	ParseTree struct of each query by pg_query_go is basically marshalled from a protobuf (Refer pg_query.Parse() function)
	We can tree each query/node as protobuf having fields and other nodes nested in it

	For example:
	Query: SELECT id, xmlelement(name "employee", name) AS employee_data FROM employees
	ParseTree: version:160001  stmts:{stmt:{
	select_stmt:{target_list:{res_target:{val:{column_ref:{fields:{string:{sval:"id"}}  location:7}}  location:7}}
	target_list:{res_target:{name:"employee_data"  val:{xml_expr:{op:IS_XMLELEMENT  name:"employee"  args:{column_ref:{fields:{string:{sval:"name"}}  location:39}}  xmloption:XMLOPTION_DOCUMENT  location:11}}  location:11}}
	from_clause:{range_var:{relname:"employees"  inh:true  relpersistence:"p"  location:67}}  limit_option:LIMIT_OPTION_DEFAULT  op:SETOP_NONE}}}

	Traversal Output:
	Traversing NodeType: pg_query.Node
	Traversing NodeType: pg_query.SelectStmt
	Field: target_list, Type: message
	Traversing NodeType: pg_query.Node
	Traversing NodeType: pg_query.ResTarget
	Field: val, Type: message
	Traversing NodeType: pg_query.Node
	Traversing NodeType: pg_query.ColumnRef
	Field: fields, Type: message
	Traversing NodeType: pg_query.Node
	Traversing NodeType: pg_query.String
	Field: sval, Type: string
	Field: location, Type: int32
	Field: location, Type: int32
	Traversing NodeType: pg_query.Node
	Traversing NodeType: pg_query.ResTarget
	Field: name, Type: string
	Field: val, Type: message
	Traversing NodeType: pg_query.Node
	Traversing NodeType: pg_query.XmlExpr
	XML Functions
	Field: op, Type: enum
	Field: name, Type: string
	Field: args, Type: message
	Traversing NodeType: pg_query.Node
	Traversing NodeType: pg_query.ColumnRef
	Field: fields, Type: message
	Traversing NodeType: pg_query.Node
	Traversing NodeType: pg_query.String
	Field: sval, Type: string
	Field: location, Type: int32
	Field: xmloption, Type: enum
	Field: location, Type: int32
	Field: location, Type: int32
	Field: from_clause, Type: message
	Traversing NodeType: pg_query.Node
	Traversing NodeType: pg_query.RangeVar
	Field: relname, Type: string
	Field: inh, Type: bool
	Field: relpersistence, Type: string
	Field: location, Type: int32
	Field: limit_option, Type: enum
	Field: op, Type: enum
*/

func TraverseParseTree(msg protoreflect.Message, visited map[protoreflect.Message]bool, processor NodeProcessor) error {
	if msg == nil || !msg.IsValid() {
		return nil
	}

	if visited[msg] {
		return nil
	}
	visited[msg] = true

	nodeType := msg.Descriptor().FullName()
	log.Debugf("Traversing NodeType: %s\n", nodeType)
	// applying the processor to the current node
	if err := processor(msg); err != nil {
		log.Debugf("error processing node %s: %v", nodeType, err)
		return fmt.Errorf("error processing node %s: %w", nodeType, err)
	}

	// Reference Oneof - https://protobuf.dev/programming-guides/proto3/#oneof
	if nodeType == PG_QUERY_NODE_NODE {
		nodeField := getOneofActiveField(msg, "node")
		if nodeField != nil {
			value := msg.Get(nodeField)
			if value.IsValid() {
				err := TraverseParseTree(value.Message(), visited, processor)
				if err != nil {
					return err
				}
			}
		}
	}

	return TraverseNodeFields(msg, visited, processor)
}

func TraverseNodeFields(msg protoreflect.Message, visited map[protoreflect.Message]bool, processor NodeProcessor) error {
	fields := msg.Descriptor().Fields() // returns declared fields, not populated ones
	if fields == nil || fields.Len() == 0 {
		return nil
	}

	for i := 0; i < fields.Len(); i++ {
		fieldDesc := fields.Get(i)

		/*
			Checks if current field is set or not.
			As per documentation,
			// Fields is a list of nested field declarations.
			Fields() FieldDescriptors
			// Oneofs is a list of nested oneof declarations.
			Oneofs() OneofDescriptors
		*/
		if !msg.Has(fieldDesc) {
			log.Debugf("field %q is not a part of the message ", fieldDesc.FullName())
			continue
		}

		// (this check might be redundant due to previous Has() check)
		// Handle oneof fields
		if oneof := fieldDesc.ContainingOneof(); oneof != nil {
			// Only process if the fieldDesc (field) is the one that is set in the oneof group
			if fieldDesc != msg.WhichOneof(oneof) {
				continue // Skip unset fields in the oneof group
			}
		}
		log.Debugf("Field: %s, Type: %s\n", fieldDesc.Name(), fieldDesc.Kind())
		value := msg.Get(fieldDesc)
		switch {
		case fieldDesc.IsList() && fieldDesc.Kind() == protoreflect.MessageKind:
			list := value.List()
			for j := 0; j < list.Len(); j++ {
				// every elem in the list have same Kind as that of field
				elem := list.Get(j)
				err := TraverseParseTree(elem.Message(), visited, processor)
				if err != nil {
					log.Debugf("error traversing field %s: %v", fieldDesc.Name(), err)
					return fmt.Errorf("error traversing field %s: %w", fieldDesc.Name(), err)
				}
			}

		case fieldDesc.Kind() == protoreflect.MessageKind:
			err := TraverseParseTree(value.Message(), visited, processor)
			if err != nil {
				log.Debugf("error traversing field %s: %v", fieldDesc.Name(), err)
				return fmt.Errorf("error traversing field %s: %w", fieldDesc.Name(), err)
			}

		case IsScalarKind(fieldDesc.Kind()):
			log.Debugf("Scalar field of Type: %s, value: %v\n", fieldDesc.Kind(), value.Interface())
		default:
			log.Debugf("field kind case not covered: %s\n", fieldDesc.Kind())
		}
	}

	return nil
}

// ScalarKind is all datatypes including enums
func IsScalarKind(kind protoreflect.Kind) bool {
	listOfScalarKinds := []protoreflect.Kind{
		// integer kinds
		protoreflect.Int32Kind, protoreflect.Uint32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind, protoreflect.Fixed32Kind,
		protoreflect.Int64Kind, protoreflect.Uint64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind, protoreflect.Fixed64Kind,
		// float, double, string, bool, bytes, enum kinds
		protoreflect.FloatKind, protoreflect.DoubleKind,
		protoreflect.BoolKind, protoreflect.StringKind, protoreflect.BytesKind, protoreflect.EnumKind}
	return slices.Contains(listOfScalarKinds, kind)
}

func GetSchemaUsed(query string) ([]string, error) {
	parseTree, err := Parse(query)
	if err != nil {
		return nil, fmt.Errorf("error parsing query: %w", err)
	}

	msg := GetProtoMessageFromParseTree(parseTree)
	visited := make(map[protoreflect.Message]bool)
	objectCollector := NewObjectCollector(TablesOnlyPredicate)
	err = TraverseParseTree(msg, visited, func(msg protoreflect.Message) error {
		objectCollector.Collect(msg)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("traversing parse tree: %w", err)
	}

	return objectCollector.GetSchemaList(), nil
}
