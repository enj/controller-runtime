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
	"errors"
	"fmt"
	"net/http"
	"reflect"

	admissionv1 "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Mutator mutates an admission request object.
//
// Unlike Defaulter, Mutator uses strategic-merge semantics to preserve fields that are present in the admission
// request but unknown to T. Mutations that cannot be proven safe are rejected. The managed mutation handler is
// intended for structured types; use a custom Handler to mutate raw or unstructured data.
type Mutator[T runtime.Object] interface {
	Mutate(ctx context.Context, obj T) error
}

// MutatorFunc implements Mutator using a function.
type MutatorFunc[T runtime.Object] func(context.Context, T) error

// Mutate calls f with the admission request object.
func (f MutatorFunc[T]) Mutate(ctx context.Context, obj T) error {
	return f(ctx, obj)
}

// WithMutator creates a new Webhook for a Mutator.
//
// The webhook decodes each request into T, snapshots it before invoking mutator, and safely projects the resulting
// typed changes onto the original request with PatchResponseViaStrategicMerge.
func WithMutator[T runtime.Object](scheme *runtime.Scheme, mutator Mutator[T]) *Webhook {
	objectType := reflect.TypeFor[T]()
	if objectType.Kind() != reflect.Pointer || objectType.Elem().Kind() != reflect.Struct {
		panic(fmt.Sprintf("mutator object type must be a pointer to a struct, got %v", objectType))
	}

	return &Webhook{
		Handler: &mutatorForType[T]{
			mutator: mutator,
			decoder: NewDecoder(scheme),
			new: func() T {
				return reflect.New(objectType.Elem()).Interface().(T)
			},
		},
	}
}

type mutatorForType[T runtime.Object] struct {
	mutator Mutator[T]
	decoder Decoder
	new     func() T
}

// Handle handles admission requests.
func (h *mutatorForType[T]) Handle(ctx context.Context, req Request) Response {
	if h.decoder == nil {
		panic("decoder should never be nil")
	}
	if h.mutator == nil {
		panic("mutator should never be nil")
	}

	if req.Operation == admissionv1.Delete {
		return Response{AdmissionResponse: admissionv1.AdmissionResponse{
			Allowed: true,
			Result: &metav1.Status{
				Code: http.StatusOK,
			},
		}}
	}

	ctx = NewContextWithRequest(ctx, req)

	obj := h.new()
	if err := h.decoder.Decode(req, obj); err != nil {
		return Errored(http.StatusBadRequest, err)
	}
	copied := obj.DeepCopyObject()
	before, ok := copied.(T)
	if !ok {
		return Errored(http.StatusInternalServerError, fmt.Errorf(
			"DeepCopyObject returned %T, expected %T",
			copied,
			obj,
		))
	}

	if err := h.mutator.Mutate(ctx, obj); err != nil {
		var apiStatus apierrors.APIStatus
		if errors.As(err, &apiStatus) {
			return validationResponseFromStatus(false, apiStatus.Status())
		}
		return Denied(err.Error())
	}

	return PatchResponseViaStrategicMerge(req.Object.Raw, before, obj)
}
