/*
Copyright 2026 The Kubernetes Authors.

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

package admission

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
)

var (
	jsonMarshalerType   = reflect.TypeFor[json.Marshaler]()
	jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()
)

// PatchResponseViaStrategicMerge creates a JSONPatch admission response containing only the changes between before
// and after, projected onto original with Kubernetes strategic-merge semantics. before must be the result of decoding
// original into T. The helper rejects a patch unless it can verify that it produces after without changing data that
// was not represented by before.
//
// Strategic merge is used only to project the typed mutation onto the original JSON. Admission responses support
// JSONPatch, so the returned response contains a JSONPatch from original to the verified projection.
//
// This helper is intentionally conservative. It rejects unstructured objects, changes to values with custom JSON
// serialization, merge lists without unique merge keys, and atomic list changes when original did not round-trip
// exactly through T. For those cases, mutate an unstructured object or construct explicit JSONPatch operations.
func PatchResponseViaStrategicMerge[T runtime.Object](original []byte, before, after T) Response {
	projected, err := projectTypedMutation(original, before, after)
	if err != nil {
		return Errored(http.StatusInternalServerError, fmt.Errorf(
			"cannot safely project typed mutation: %w; operate on raw or unstructured data and return explicit JSONPatch operations instead",
			err,
		))
	}
	return PatchResponseFromRaw(original, projected)
}

type strategicJSONType struct {
	typ       reflect.Type
	patchMeta strategicpatch.PatchMeta
}

func projectTypedMutation[T runtime.Object](original []byte, before, after T) ([]byte, error) {
	beforeType, err := matchingObjectType(before, after)
	if err != nil {
		return nil, err
	}

	beforeJSON, err := json.Marshal(before)
	if err != nil {
		return nil, fmt.Errorf("marshal before object: %w", err)
	}
	afterJSON, err := json.Marshal(after)
	if err != nil {
		return nil, fmt.Errorf("marshal after object: %w", err)
	}

	decodedBeforeJSON, err := decodeAndMarshalObject(original, beforeType)
	if err != nil {
		return nil, fmt.Errorf("decode original object: %w", err)
	}
	if equal, err := semanticallyEqualJSON(decodedBeforeJSON, beforeJSON); err != nil {
		return nil, err
	} else if !equal {
		return nil, fmt.Errorf("before object does not match original")
	}

	beforeValue, err := decodeJSON(beforeJSON)
	if err != nil {
		return nil, fmt.Errorf("decode marshaled before object: %w", err)
	}
	afterValue, err := decodeJSON(afterJSON)
	if err != nil {
		return nil, fmt.Errorf("decode marshaled after object: %w", err)
	}
	if reflect.DeepEqual(beforeValue, afterValue) {
		return original, nil
	}

	if _, isUnstructured := any(before).(runtime.Unstructured); isUnstructured {
		return nil, fmt.Errorf("unstructured objects do not provide strategic-merge metadata")
	}

	schema, err := strategicpatch.NewPatchMetaFromStruct(before)
	if err != nil {
		return nil, fmt.Errorf("read strategic-merge metadata: %w", err)
	}
	strategicDelta, err := strategicpatch.CreateTwoWayMergePatchUsingLookupPatchMeta(beforeJSON, afterJSON, schema)
	if err != nil {
		return nil, fmt.Errorf("create strategic merge patch: %w", err)
	}
	projected, err := strategicpatch.StrategicMergePatchUsingLookupPatchMeta(original, strategicDelta, schema)
	if err != nil {
		return nil, fmt.Errorf("apply strategic merge patch: %w", err)
	}

	decodedAfterJSON, err := decodeAndMarshalObject(projected, beforeType)
	if err != nil {
		return nil, fmt.Errorf("decode projected object: %w", err)
	}
	if equal, err := semanticallyEqualJSON(decodedAfterJSON, afterJSON); err != nil {
		return nil, err
	} else if !equal {
		return nil, fmt.Errorf("projected object does not match after object")
	}

	originalValue, err := decodeJSON(original)
	if err != nil {
		return nil, fmt.Errorf("decode original JSON: %w", err)
	}
	projectedValue, err := decodeJSON(projected)
	if err != nil {
		return nil, fmt.Errorf("decode projected JSON: %w", err)
	}
	if err := validateStrategicProjection(
		originalValue,
		true,
		projectedValue,
		true,
		beforeValue,
		true,
		afterValue,
		true,
		strategicJSONType{typ: beforeType.Elem()},
		"",
	); err != nil {
		return nil, err
	}

	return projected, nil
}

func matchingObjectType[T runtime.Object](before, after T) (reflect.Type, error) {
	beforeType := reflect.TypeOf(before)
	afterType := reflect.TypeOf(after)
	if beforeType == nil || isNilValue(reflect.ValueOf(before)) {
		return nil, fmt.Errorf("before object must not be nil")
	}
	if afterType == nil || isNilValue(reflect.ValueOf(after)) {
		return nil, fmt.Errorf("after object must not be nil")
	}
	if beforeType != afterType {
		return nil, fmt.Errorf("before and after objects must have the same concrete type, got %v and %v", beforeType, afterType)
	}
	if beforeType.Kind() != reflect.Pointer || beforeType.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("object type must be a pointer to a struct, got %v", beforeType)
	}
	return beforeType, nil
}

func isNilValue(value reflect.Value) bool {
	if value.Kind() == reflect.Chan ||
		value.Kind() == reflect.Func ||
		value.Kind() == reflect.Interface ||
		value.Kind() == reflect.Map ||
		value.Kind() == reflect.Pointer ||
		value.Kind() == reflect.Slice {
		return value.IsNil()
	}
	return false
}

func decodeAndMarshalObject(data []byte, objectType reflect.Type) ([]byte, error) {
	object := reflect.New(objectType.Elem()).Interface()
	if err := json.Unmarshal(data, object); err != nil {
		return nil, err
	}
	return json.Marshal(object)
}

func semanticallyEqualJSON(left, right []byte) (bool, error) {
	leftValue, err := decodeJSON(left)
	if err != nil {
		return false, fmt.Errorf("decode left JSON: %w", err)
	}
	rightValue, err := decodeJSON(right)
	if err != nil {
		return false, fmt.Errorf("decode right JSON: %w", err)
	}
	return reflect.DeepEqual(leftValue, rightValue), nil
}

func decodeJSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	return value, nil
}

func validateStrategicProjection(
	original any,
	originalExists bool,
	projected any,
	projectedExists bool,
	before any,
	beforeExists bool,
	after any,
	afterExists bool,
	valueType strategicJSONType,
	path string,
) error {
	if beforeExists == afterExists && reflect.DeepEqual(before, after) {
		if originalExists != projectedExists || !reflect.DeepEqual(original, projected) {
			return unsafeStrategicChange(path, "unchanged typed value changed in the projected JSON")
		}
		return nil
	}

	if !beforeExists {
		if !afterExists || !projectedExists {
			return unsafeStrategicChange(path, "typed addition is missing from the projected JSON")
		}
		if originalExists && isNonEmptyJSONComposite(original) {
			return unsafeStrategicChange(path, "typed addition would replace unrepresented composite data")
		}
		if !reflect.DeepEqual(projected, after) {
			return unsafeStrategicChange(path, "typed addition does not match the projected JSON")
		}
		return nil
	}
	if !afterExists {
		if projectedExists {
			return unsafeStrategicChange(path, "typed removal remains in the projected JSON")
		}
		return nil
	}
	if !projectedExists {
		return unsafeStrategicChange(path, "typed value is missing from the projected JSON")
	}

	if hasCustomJSONSerialization(valueType.typ) {
		return unsafeStrategicChange(path, "changed value uses custom JSON serialization")
	}
	indirectValueType := indirectType(valueType.typ)
	if indirectValueType == nil {
		return unsafeStrategicChange(path, "cannot resolve changed value type")
	}
	if indirectValueType.Kind() == reflect.Interface {
		return unsafeStrategicChange(path, "changed value has an interface type")
	}

	switch typedBefore := before.(type) {
	case map[string]any:
		typedAfter, ok := after.(map[string]any)
		if !ok {
			return requireProjectedValue(path, projected, after)
		}
		rawMap := map[string]any{}
		if originalExists && original != nil {
			rawMap, ok = original.(map[string]any)
			if !ok {
				return unsafeStrategicChange(path, "typed object does not match original JSON")
			}
		}
		projectedMap, ok := projected.(map[string]any)
		if !ok {
			return unsafeStrategicChange(path, "typed object does not match projected JSON")
		}
		return validateStrategicMap(rawMap, projectedMap, typedBefore, typedAfter, valueType, path)
	case []any:
		typedAfter, ok := after.([]any)
		if !ok {
			return requireProjectedValue(path, projected, after)
		}
		rawSlice := []any{}
		if originalExists {
			rawSlice, ok = original.([]any)
			if !ok {
				return unsafeStrategicChange(path, "typed list does not match original JSON")
			}
		}
		projectedSlice, ok := projected.([]any)
		if !ok {
			return unsafeStrategicChange(path, "typed list does not match projected JSON")
		}
		return validateStrategicSlice(rawSlice, projectedSlice, typedBefore, typedAfter, valueType, path)
	default:
		return requireProjectedValue(path, projected, after)
	}
}

func validateStrategicMap(
	original, projected, before, after map[string]any,
	valueType strategicJSONType,
	path string,
) error {
	keys := make(map[string]struct{}, len(original)+len(projected)+len(before)+len(after))
	for key := range original {
		keys[key] = struct{}{}
	}
	for key := range projected {
		keys[key] = struct{}{}
	}
	for key := range before {
		keys[key] = struct{}{}
	}
	for key := range after {
		keys[key] = struct{}{}
	}

	sortedKeys := make([]string, 0, len(keys))
	for key := range keys {
		sortedKeys = append(sortedKeys, key)
	}
	slices.Sort(sortedKeys)

	for _, key := range sortedKeys {
		beforeValue, beforeExists := before[key]
		afterValue, afterExists := after[key]
		originalValue, originalExists := original[key]
		projectedValue, projectedExists := projected[key]

		childType := strategicJSONType{}
		if beforeExists || afterExists {
			var err error
			childType, err = strategicChildType(valueType.typ, key)
			if err != nil {
				return unsafeStrategicChange(jsonPointer(path, key), "cannot resolve field metadata: %v", err)
			}
		}

		if err := validateStrategicProjection(
			originalValue,
			originalExists,
			projectedValue,
			projectedExists,
			beforeValue,
			beforeExists,
			afterValue,
			afterExists,
			childType,
			jsonPointer(path, key),
		); err != nil {
			return err
		}
	}
	return nil
}

func validateStrategicSlice(
	original, projected, before, after []any,
	valueType strategicJSONType,
	path string,
) error {
	if !hasPatchStrategy(valueType.patchMeta, "merge") || valueType.patchMeta.GetPatchMergeKey() == "" {
		if !reflect.DeepEqual(original, before) {
			return unsafeStrategicChange(path, "atomic list does not exactly match its typed representation")
		}
		if !reflect.DeepEqual(projected, after) {
			return unsafeStrategicChange(path, "atomic list replacement does not match the requested value")
		}
		return nil
	}

	mergeKey := valueType.patchMeta.GetPatchMergeKey()
	originalItems, err := indexMergeList(original, mergeKey)
	if err != nil {
		return unsafeStrategicChange(path, "invalid original merge list: %v", err)
	}
	projectedItems, err := indexMergeList(projected, mergeKey)
	if err != nil {
		return unsafeStrategicChange(path, "invalid projected merge list: %v", err)
	}
	beforeItems, err := indexMergeList(before, mergeKey)
	if err != nil {
		return unsafeStrategicChange(path, "invalid before merge list: %v", err)
	}
	afterItems, err := indexMergeList(after, mergeKey)
	if err != nil {
		return unsafeStrategicChange(path, "invalid after merge list: %v", err)
	}

	if !sameKeys(originalItems, beforeItems) {
		return unsafeStrategicChange(path, "original merge-list keys do not match the typed before value")
	}
	if !sameKeys(projectedItems, afterItems) {
		return unsafeStrategicChange(path, "projected merge-list keys do not match the typed after value")
	}

	listType := indirectType(valueType.typ)
	if listType == nil || (listType.Kind() != reflect.Slice && listType.Kind() != reflect.Array) {
		return unsafeStrategicChange(path, "cannot resolve merge-list element type")
	}
	elementType := strategicJSONType{typ: listType.Elem()}
	for key, beforeItem := range beforeItems {
		afterItem, remains := afterItems[key]
		originalItem := originalItems[key]
		projectedItem, projectedExists := projectedItems[key]
		if !remains {
			if projectedExists {
				return unsafeStrategicChange(path, "removed merge-list item %q remains in projected JSON", key)
			}
			continue
		}
		if !projectedExists {
			return unsafeStrategicChange(path, "merge-list item %q is missing from projected JSON", key)
		}
		if err := validateStrategicProjection(
			originalItem,
			true,
			projectedItem,
			true,
			beforeItem,
			true,
			afterItem,
			true,
			elementType,
			jsonPointer(path, key),
		); err != nil {
			return err
		}
	}

	for key, afterItem := range afterItems {
		if _, existed := beforeItems[key]; existed {
			continue
		}
		projectedItem, projectedExists := projectedItems[key]
		if err := validateStrategicProjection(
			nil,
			false,
			projectedItem,
			projectedExists,
			nil,
			false,
			afterItem,
			true,
			elementType,
			jsonPointer(path, key),
		); err != nil {
			return err
		}
	}
	return nil
}

func strategicChildType(parentType reflect.Type, key string) (strategicJSONType, error) {
	parentType = indirectType(parentType)
	if parentType == nil {
		return strategicJSONType{}, fmt.Errorf("parent type is unknown")
	}
	if parentType.Kind() == reflect.Struct {
		child, patchMeta, err := (strategicpatch.PatchMetaFromStruct{T: parentType}).LookupPatchMetadataForStruct(key)
		if err != nil {
			return strategicJSONType{}, err
		}
		childFromStruct, ok := child.(strategicpatch.PatchMetaFromStruct)
		if !ok {
			return strategicJSONType{}, fmt.Errorf("unexpected metadata type %T", child)
		}
		return strategicJSONType{typ: childFromStruct.T, patchMeta: patchMeta}, nil
	}
	if parentType.Kind() == reflect.Map {
		return strategicJSONType{typ: parentType.Elem()}, nil
	}
	return strategicJSONType{}, fmt.Errorf("expected struct or map, got %v", parentType)
}

func hasPatchStrategy(patchMeta strategicpatch.PatchMeta, strategy string) bool {
	return slices.Contains(patchMeta.GetPatchStrategies(), strategy)
}

func indexMergeList(items []any, mergeKey string) (map[string]any, error) {
	indexed := make(map[string]any, len(items))
	for _, item := range items {
		itemMap, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("item has type %T, expected object", item)
		}
		keyValue, ok := itemMap[mergeKey]
		if !ok {
			return nil, fmt.Errorf("item is missing merge key %q", mergeKey)
		}
		key, err := mergeKeyID(keyValue)
		if err != nil {
			return nil, fmt.Errorf("merge key %q: %w", mergeKey, err)
		}
		if _, duplicate := indexed[key]; duplicate {
			return nil, fmt.Errorf("merge key %q is not unique", keyValue)
		}
		indexed[key] = item
	}
	return indexed, nil
}

func mergeKeyID(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return "string:" + typed, nil
	case json.Number:
		return "number:" + typed.String(), nil
	case bool:
		return fmt.Sprintf("bool:%t", typed), nil
	default:
		return "", fmt.Errorf("value has unsupported type %T", value)
	}
}

func sameKeys(left, right map[string]any) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if _, ok := right[key]; !ok {
			return false
		}
	}
	return true
}

func hasCustomJSONSerialization(typ reflect.Type) bool {
	if typ == nil {
		return false
	}
	if typ.Implements(jsonMarshalerType) || typ.Implements(jsonUnmarshalerType) {
		return true
	}
	typ = indirectType(typ)
	if typ == nil {
		return false
	}
	return typ.Implements(jsonMarshalerType) ||
		typ.Implements(jsonUnmarshalerType) ||
		reflect.PointerTo(typ).Implements(jsonMarshalerType) ||
		reflect.PointerTo(typ).Implements(jsonUnmarshalerType)
}

func indirectType(typ reflect.Type) reflect.Type {
	for typ != nil && typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ
}

func isNonEmptyJSONComposite(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		return len(typed) != 0
	case []any:
		return len(typed) != 0
	default:
		return false
	}
}

func requireProjectedValue(path string, projected, after any) error {
	if !reflect.DeepEqual(projected, after) {
		return unsafeStrategicChange(path, "projected value does not match the requested typed value")
	}
	return nil
}

func unsafeStrategicChange(path, format string, args ...any) error {
	if path == "" {
		path = "/"
	}
	return fmt.Errorf("unsafe strategic-merge change at %s: %s", path, fmt.Sprintf(format, args...))
}

func jsonPointer(parent, key string) string {
	key = strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
	return parent + "/" + key
}
