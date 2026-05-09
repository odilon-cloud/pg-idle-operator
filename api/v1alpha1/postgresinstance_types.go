// api/v1alpha1/postgresinstance_types.go
//
// Defines the PostgresInstance CRD — the custom resource that
// operators declare to get a managed, auto-pausing Postgres instance.
//
// Users apply something like:
//
//   kubectl apply -f my-tenant-db.yaml
//
// and the operator takes care of provisioning the StatefulSet,
// monitoring for idleness, and scaling to zero when appropriate.

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PostgresInstanceSpec defines the desired state of a PostgresInstance.
type PostgresInstanceSpec struct {
	// Version is the Postgres major version to run, e.g. "15", "16".
	// +kubebuilder:validation:Pattern=`^\d+$`
	Version string `json:"version"`

	// Storage is the PVC size for this instance, e.g. "10Gi".
	Storage string `json:"storage"`

	// IdleTimeoutMinutes is how long (in minutes) the instance must be
	// connection-free before the operator scales it to zero.
	// Defaults to 10 if unset.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	IdleTimeoutMinutes int32 `json:"idleTimeoutMinutes,omitempty"`

	// Paused allows an operator or user to manually freeze reconciliation.
	// Useful during maintenance windows.
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// InstancePhase describes the current lifecycle phase of the instance.
// +kubebuilder:validation:Enum=Running;Idle;Paused;Provisioning;Error
type InstancePhase string

const (
	PhaseProvisioning InstancePhase = "Provisioning"
	PhaseRunning      InstancePhase = "Running"
	PhaseIdle         InstancePhase = "Idle"   // approaching scale-to-zero
	PhasePaused       InstancePhase = "Paused" // scaled to zero
	PhaseError        InstancePhase = "Error"
)

// PostgresInstanceStatus is written back by the controller.
// Users and tooling read this to understand the live state.
type PostgresInstanceStatus struct {
	// Phase is the current lifecycle phase.
	Phase InstancePhase `json:"phase,omitempty"`

	// LastActiveTime is the last time at least one active connection was
	// observed. The controller uses this to calculate idle duration.
	// +optional
	LastActiveTime *metav1.Time `json:"lastActiveTime,omitempty"`

	// ActiveConnections is the connection count observed on the last poll.
	ActiveConnections int32 `json:"activeConnections,omitempty"`

	// Conditions follows the standard Kubernetes condition pattern,
	// giving structured reason/message fields for each state transition.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Connections",type=integer,JSONPath=`.status.activeConnections`
// +kubebuilder:printcolumn:name="Last Active",type=date,JSONPath=`.status.lastActiveTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// PostgresInstance is the Schema for the postgresinstances API.
type PostgresInstance struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PostgresInstanceSpec   `json:"spec,omitempty"`
	Status PostgresInstanceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PostgresInstanceList contains a list of PostgresInstance.
type PostgresInstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PostgresInstance `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PostgresInstance{}, &PostgresInstanceList{})
}
