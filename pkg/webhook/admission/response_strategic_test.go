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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"

	jsonpatch "github.com/evanphx/json-patch/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ = Describe("Strategic merge admission responses", func() {
	Describe("PatchResponseViaStrategicMerge", func() {
		It("preserves raw-only fields in nested Pod merge lists", func() {
			original := []byte(`{
				"apiVersion":"v1",
				"kind":"Pod",
				"metadata":{"name":"workload"},
				"spec":{
					"futurePodField":{"nested":"keep"},
					"containers":[{
						"name":"application",
						"image":"old",
						"futureContainerField":"keep",
						"env":[{
							"name":"SETTING",
							"value":"old",
							"futureEnvField":"keep"
						}]
					}]
				}
			}`)
			before := &corev1.Pod{}
			Expect(json.Unmarshal(original, before)).To(Succeed())
			after := before.DeepCopy()
			after.Spec.Containers[0].Image = "new"
			after.Spec.Containers[0].Env[0].Value = "new"

			projected, err := projectTypedMutation(original, before, after)
			Expect(err).NotTo(HaveOccurred())
			Expect(projected).To(MatchJSON(`{
				"apiVersion":"v1",
				"kind":"Pod",
				"metadata":{"name":"workload"},
				"spec":{
					"futurePodField":{"nested":"keep"},
					"containers":[{
						"name":"application",
						"image":"new",
						"futureContainerField":"keep",
						"env":[{
							"name":"SETTING",
							"value":"new",
							"futureEnvField":"keep"
						}]
					}]
				}
			}`))
		})

		It("preserves raw-only Pod fields and fields in merge-list items", func() {
			original := []byte(`{
				"apiVersion":"v1",
				"kind":"Pod",
				"metadata":{"name":"workload"},
				"spec":{
					"resources":{"limits":{"cpu":"2"}},
					"containers":[
						{"name":"application","image":"old","futureContainerField":"keep-application"},
						{"name":"sidecar","image":"sidecar","futureContainerField":"keep-sidecar"}
					]
				}
			}`)
			before := decodeStrategicTestObject(original)
			after := before.DeepCopyObject().(*strategicTestObject)
			after.Spec.Containers[0].Image = "new"
			slices.Reverse(after.Spec.Containers)

			projected, err := projectTypedMutation(original, before, after)
			Expect(err).NotTo(HaveOccurred())
			Expect(projected).To(MatchJSON(`{
				"apiVersion":"v1",
				"kind":"Pod",
				"metadata":{"name":"workload"},
				"spec":{
					"resources":{"limits":{"cpu":"2"}},
					"containers":[
						{"name":"sidecar","image":"sidecar","futureContainerField":"keep-sidecar"},
						{"name":"application","image":"new","futureContainerField":"keep-application"}
					]
				}
			}`))

			response := PatchResponseViaStrategicMerge(original, before, after)
			Expect(response.Allowed).To(BeTrue())
			Expect(response.Patches).NotTo(BeEmpty())
			Expect(applyResponsePatch(original, response)).To(MatchJSON(projected))
		})

		It("adds a typed field below an omitted parent", func() {
			original := []byte(`{
				"apiVersion":"test.controller-runtime.io/v1",
				"kind":"StrategicTestObject",
				"spec":{"unknown":"keep"}
			}`)
			before := decodeStrategicTestObject(original)
			after := before.DeepCopyObject().(*strategicTestObject)
			after.Spec.Settings.Enabled = true

			projected, err := projectTypedMutation(original, before, after)
			Expect(err).NotTo(HaveOccurred())
			Expect(projected).To(MatchJSON(`{
				"apiVersion":"test.controller-runtime.io/v1",
				"kind":"StrategicTestObject",
				"spec":{"settings":{"enabled":true},"unknown":"keep"}
			}`))
		})

		It("safely replaces an explicit null and preserves unknown lists", func() {
			original := []byte(`{
				"apiVersion":"test.controller-runtime.io/v1",
				"kind":"StrategicTestObject",
				"spec":{
					"optional":null,
					"futureList":[{"value":"keep"}]
				}
			}`)
			before := decodeStrategicTestObject(original)
			after := before.DeepCopyObject().(*strategicTestObject)
			after.Spec.Optional = &strategicTestSettings{Enabled: true}

			projected, err := projectTypedMutation(original, before, after)
			Expect(err).NotTo(HaveOccurred())
			Expect(projected).To(MatchJSON(`{
				"apiVersion":"test.controller-runtime.io/v1",
				"kind":"StrategicTestObject",
				"spec":{
					"optional":{"enabled":true},
					"futureList":[{"value":"keep"}]
				}
			}`))
		})

		It("allows intentional removal of a merge-list item", func() {
			original := []byte(`{
				"apiVersion":"v1",
				"kind":"Pod",
				"spec":{"containers":[
					{"name":"remove","image":"old","futureContainerField":"removed-too"},
					{"name":"keep","image":"old","futureContainerField":"preserved"}
				]}
			}`)
			before := decodeStrategicTestObject(original)
			after := before.DeepCopyObject().(*strategicTestObject)
			after.Spec.Containers = after.Spec.Containers[1:]

			projected, err := projectTypedMutation(original, before, after)
			Expect(err).NotTo(HaveOccurred())
			Expect(projected).To(MatchJSON(`{
				"apiVersion":"v1",
				"kind":"Pod",
				"spec":{"containers":[
					{"name":"keep","image":"old","futureContainerField":"preserved"}
				]}
			}`))
		})

		It("allows an atomic-list replacement after an exact typed round trip", func() {
			original := []byte(`{
				"apiVersion":"v1",
				"kind":"Pod",
				"spec":{"atomic":[{"name":"one","image":"old"}]}
			}`)
			before := decodeStrategicTestObject(original)
			after := before.DeepCopyObject().(*strategicTestObject)
			after.Spec.Atomic[0].Image = "new"

			projected, err := projectTypedMutation(original, before, after)
			Expect(err).NotTo(HaveOccurred())
			Expect(projected).To(MatchJSON(`{
				"apiVersion":"v1",
				"kind":"Pod",
				"spec":{"atomic":[{"name":"one","image":"new"}]}
			}`))
		})

		It("rejects an atomic-list change that would lose a raw-only field", func() {
			original := []byte(`{
				"apiVersion":"v1",
				"kind":"Pod",
				"spec":{"atomic":[{"name":"one","image":"old","future":"keep"}]}
			}`)
			before := decodeStrategicTestObject(original)
			after := before.DeepCopyObject().(*strategicTestObject)
			after.Spec.Atomic[0].Image = "new"

			_, err := projectTypedMutation(original, before, after)
			Expect(err).To(MatchError(ContainSubstring("atomic list does not exactly match its typed representation")))
		})

		It("rejects duplicate merge keys", func() {
			original := []byte(`{
				"apiVersion":"v1",
				"kind":"Pod",
				"spec":{"containers":[
					{"name":"duplicate","image":"one"},
					{"name":"duplicate","image":"two"}
				]}
			}`)
			before := decodeStrategicTestObject(original)
			after := before.DeepCopyObject().(*strategicTestObject)
			after.Spec.Containers[0].Image = "changed"

			_, err := projectTypedMutation(original, before, after)
			Expect(err).To(MatchError(ContainSubstring(`merge key "duplicate" is not unique`)))
		})

		It("rejects changed values with custom JSON serialization", func() {
			original := []byte(`{
				"apiVersion":"v1",
				"kind":"Pod",
				"spec":{"timestamp":"2026-08-29T00:00:00Z"}
			}`)
			before := decodeStrategicTestObject(original)
			after := before.DeepCopyObject().(*strategicTestObject)
			after.Spec.Timestamp = metav1.NewTime(after.Spec.Timestamp.AddDate(0, 0, 1))

			_, err := projectTypedMutation(original, before, after)
			Expect(err).To(MatchError(ContainSubstring("changed value uses custom JSON serialization")))
		})

		It("rejects changed unstructured objects", func() {
			original := []byte(`{"value":"before","unknown":"keep"}`)
			before := &unstructured.Unstructured{Object: map[string]any{"value": "before", "unknown": "keep"}}
			after := before.DeepCopy()
			after.Object["value"] = "after"

			response := PatchResponseViaStrategicMerge(original, before, after)
			Expect(response.Allowed).To(BeFalse())
			Expect(response.Result.Code).To(Equal(int32(http.StatusInternalServerError)))
			Expect(response.Result.Message).To(ContainSubstring("mutate an unstructured object or return explicit JSONPatch operations"))
		})

		It("rejects mismatched concrete types and before values", func() {
			original := []byte(`{"spec":{"value":"original"}}`)
			before := decodeStrategicTestObject(original)

			response := PatchResponseViaStrategicMerge[runtime.Object](original, before, &otherStrategicTestObject{})
			Expect(response.Allowed).To(BeFalse())
			Expect(response.Result.Message).To(ContainSubstring("same concrete type"))

			after := before.DeepCopyObject().(*strategicTestObject)
			before.Spec.Value = "not-original"
			response = PatchResponseViaStrategicMerge(original, before, after)
			Expect(response.Allowed).To(BeFalse())
			Expect(response.Result.Message).To(ContainSubstring("before object does not match original"))
		})

		It("returns no patch for a no-op while preserving raw JSON", func() {
			original := []byte(`{"spec":{"unknown":["keep"],"settings":null}}`)
			before := decodeStrategicTestObject(original)

			response := PatchResponseViaStrategicMerge(original, before, before.DeepCopyObject().(*strategicTestObject))
			Expect(response.Allowed).To(BeTrue())
			Expect(response.Patches).To(BeEmpty())
		})
	})

	Describe("WithMutator", func() {
		var scheme *runtime.Scheme

		BeforeEach(func() {
			scheme = runtime.NewScheme()
			scheme.AddKnownTypeWithName(
				strategicTestGroupVersion.WithKind("StrategicTestObject"),
				&strategicTestObject{},
			)
			metav1.AddToGroupVersion(scheme, strategicTestGroupVersion)
		})

		It("owns decoding, snapshotting, mutation, and safe response generation", func(ctx SpecContext) {
			original := []byte(`{
				"apiVersion":"test.controller-runtime.io/v1",
				"kind":"StrategicTestObject",
				"spec":{
					"value":"before",
					"unknown":{"nested":"keep"}
				}
			}`)
			handler := WithMutator(scheme, MutatorFunc[*strategicTestObject](func(ctx context.Context, obj *strategicTestObject) error {
				req, err := RequestFromContext(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(req.Operation).To(Equal(admissionv1.Create))
				obj.Spec.Value = "after"
				return nil
			}))

			response := handler.Handle(ctx, strategicTestRequest(admissionv1.Create, original))
			Expect(response.Allowed).To(BeTrue())
			Expect(response.PatchType).To(HaveValue(Equal(admissionv1.PatchTypeJSONPatch)))
			Expect(applyResponsePatch(original, response)).To(MatchJSON(`{
				"apiVersion":"test.controller-runtime.io/v1",
				"kind":"StrategicTestObject",
				"spec":{
					"value":"after",
					"unknown":{"nested":"keep"}
				}
			}`))
		})

		It("fails closed when a mutation cannot be projected safely", func(ctx SpecContext) {
			original := []byte(`{
				"apiVersion":"test.controller-runtime.io/v1",
				"kind":"StrategicTestObject",
				"spec":{"atomic":[{"name":"one","image":"old","future":"keep"}]}
			}`)
			handler := WithMutator(scheme, MutatorFunc[*strategicTestObject](func(_ context.Context, obj *strategicTestObject) error {
				obj.Spec.Atomic[0].Image = "new"
				return nil
			}))

			response := handler.Handle(ctx, strategicTestRequest(admissionv1.Update, original))
			Expect(response.Allowed).To(BeFalse())
			Expect(response.Result.Code).To(Equal(int32(http.StatusInternalServerError)))
			Expect(response.Result.Message).To(ContainSubstring("cannot safely project typed mutation"))
		})

		It("skips deletes without invoking the mutator", func(ctx SpecContext) {
			handler := WithMutator(scheme, MutatorFunc[*strategicTestObject](func(context.Context, *strategicTestObject) error {
				return errors.New("must not be called")
			}))

			response := handler.Handle(ctx, strategicTestRequest(admissionv1.Delete, nil))
			Expect(response.Allowed).To(BeTrue())
			Expect(response.Result.Code).To(Equal(int32(http.StatusOK)))
		})

		It("propagates API status errors and rejects malformed requests", func(ctx SpecContext) {
			handler := WithMutator(scheme, MutatorFunc[*strategicTestObject](func(context.Context, *strategicTestObject) error {
				return apierrors.NewBadRequest("invalid mutation")
			}))

			response := handler.Handle(ctx, strategicTestRequest(admissionv1.Create, []byte(`{
				"apiVersion":"test.controller-runtime.io/v1",
				"kind":"StrategicTestObject"
			}`)))
			Expect(response.Allowed).To(BeFalse())
			Expect(response.Result.Code).To(Equal(int32(http.StatusBadRequest)))
			Expect(response.Result.Message).To(ContainSubstring("invalid mutation"))

			response = handler.Handle(ctx, strategicTestRequest(admissionv1.Create, []byte(`{`)))
			Expect(response.Allowed).To(BeFalse())
			Expect(response.Result.Code).To(Equal(int32(http.StatusBadRequest)))
		})
	})
})

var strategicTestGroupVersion = schema.GroupVersion{Group: "test.controller-runtime.io", Version: "v1"}

type strategicTestObject struct {
	metav1.TypeMeta `json:",inline"`
	Spec            strategicTestSpec `json:"spec,omitempty"`
}

type strategicTestSpec struct {
	Value      string                 `json:"value,omitempty"`
	Settings   strategicTestSettings  `json:"settings"`
	Optional   *strategicTestSettings `json:"optional,omitempty"`
	Containers []strategicTestItem    `json:"containers,omitempty" patchStrategy:"merge" patchMergeKey:"name"`
	Atomic     []strategicTestItem    `json:"atomic,omitempty"`
	Timestamp  metav1.Time            `json:"timestamp,omitempty"`
}

type strategicTestSettings struct {
	Enabled bool `json:"enabled,omitempty"`
}

type strategicTestItem struct {
	Name  string `json:"name"`
	Image string `json:"image,omitempty"`
}

func (o *strategicTestObject) DeepCopyObject() runtime.Object {
	copy := *o
	copy.Spec.Optional = nil
	if o.Spec.Optional != nil {
		optional := *o.Spec.Optional
		copy.Spec.Optional = &optional
	}
	copy.Spec.Containers = slices.Clone(o.Spec.Containers)
	copy.Spec.Atomic = slices.Clone(o.Spec.Atomic)
	return &copy
}

type otherStrategicTestObject struct {
	metav1.TypeMeta `json:",inline"`
}

func (o *otherStrategicTestObject) DeepCopyObject() runtime.Object {
	copy := *o
	return &copy
}

func decodeStrategicTestObject(data []byte) *strategicTestObject {
	GinkgoHelper()
	object := &strategicTestObject{}
	Expect(json.Unmarshal(data, object)).To(Succeed())
	return object
}

func applyResponsePatch(original []byte, response Response) []byte {
	GinkgoHelper()
	patchData, err := json.Marshal(response.Patches)
	Expect(err).NotTo(HaveOccurred())
	patch, err := jsonpatch.DecodePatch(patchData)
	Expect(err).NotTo(HaveOccurred())
	projected, err := patch.Apply(original)
	Expect(err).NotTo(HaveOccurred())
	return projected
}

func strategicTestRequest(operation admissionv1.Operation, raw []byte) Request {
	return Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: operation,
		Object:    runtime.RawExtension{Raw: raw},
	}}
}
