/*
Copyright 2024.

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

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	v1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"time"

	apiv1alpha1 "github.com/cai648971528/scaler-deploy/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	finalizer = "scalers.api.scaler.com/finalizer"
)

var logger = log.Log.WithName("scaler controller")

var originalDeploymentInfo = make(map[string]apiv1alpha1.DeploymentInfo)
var annotationMap = make(map[string]string)

// ScalerReconciler reconciles a Scaler object
type ScalerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=api.scaler.com,resources=scalers,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=api.scaler.com,resources=scalers/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=api.scaler.com,resources=scalers/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Scaler object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.12.2/pkg/reconcile
func (r *ScalerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logger.WithValues("request.namespace", req.Namespace, "request.name", req.Name)
	log.Info("scaler call")

	scaler := &apiv1alpha1.Scaler{}

	err := r.Get(ctx, req.NamespacedName, scaler)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if scaler.ObjectMeta.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(scaler, finalizer) {
			controllerutil.AddFinalizer(scaler, finalizer)
			err := r.Update(ctx, scaler)
			if err != nil {
				return ctrl.Result{}, err
			}
		}

		if scaler.Status.Status == "" {
			scaler.Status.Status = apiv1alpha1.PENDING
			err := r.Status().Update(ctx, scaler)
			if err != nil {
				return ctrl.Result{}, client.IgnoreNotFound(err)
			}

			//将scaler管理的deployment的原始状态更新到annotation
			err = addAnnotation(ctx, scaler, r)
			if err != nil {
				return ctrl.Result{}, client.IgnoreNotFound(err)
			}
		}

		//开始执行scaler的逻辑
		startTime := scaler.Spec.Start
		endTime := scaler.Spec.End
		replicas := scaler.Spec.Replicas

		currentHour := time.Now().Local().Hour()
		log.Info(fmt.Sprintf("currentTime %d", currentHour))

		if currentHour >= startTime && currentHour < endTime {
			if scaler.Status.Status != apiv1alpha1.SCALER {
				log.Info("starting to call scale Deployment func.")
				err := scalerDeployment(ctx, scaler, r, replicas)
				if err != nil {
					return ctrl.Result{}, err
				}
			}
		} else {
			if scaler.Status.Status == apiv1alpha1.SCALER {
				err := restoredDeployment(ctx, scaler, r)
				if err != nil {
					return ctrl.Result{}, err
				}
			}
		}

	} else {
		log.Info("starting deletion flow.")
		if scaler.Status.Status == apiv1alpha1.SCALER {
			err := restoredDeployment(ctx, scaler, r)
			if err != nil {
				return ctrl.Result{}, err
			}
			log.Info("remove finalizer")
			controllerutil.RemoveFinalizer(scaler, finalizer)
			err = r.Update(ctx, scaler)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		log.Info("remove scaler")
	}

	return ctrl.Result{RequeueAfter: time.Second * 10}, nil
}

func scalerDeployment(ctx context.Context, scaler *apiv1alpha1.Scaler, r *ScalerReconciler, replicas int32) error {
	logger.Info("starting scale deployment.")
	for _, deploy := range scaler.Spec.Deployments {
		deployment := &v1.Deployment{}
		err := r.Get(ctx, types.NamespacedName{
			Name:      deploy.Name,
			Namespace: deploy.Namespace,
		}, deployment)
		if err != nil {
			return err
		}

		if deployment.Spec.Replicas != &replicas {
			deployment.Spec.Replicas = &replicas
			err := r.Update(ctx, deployment)
			if err != nil {
				scaler.Status.Status = apiv1alpha1.FAILED
				r.Status().Update(ctx, scaler)
				return err
			}
			scaler.Status.Status = apiv1alpha1.SCALER
			r.Status().Update(ctx, scaler)
		}
	}
	return nil
}

func restoredDeployment(ctx context.Context, scaler *apiv1alpha1.Scaler, r *ScalerReconciler) error {
	logger.Info("starting to return original replicas.")
	for name, originDeployInfo := range originalDeploymentInfo {
		deployment := &v1.Deployment{}
		err := r.Get(ctx, types.NamespacedName{
			Namespace: originDeployInfo.Namespace,
			Name:      name,
		}, deployment)
		if err != nil {
			return err
		}

		if deployment.Spec.Replicas != &originDeployInfo.Replicas {
			deployment.Spec.Replicas = &originDeployInfo.Replicas
			err := r.Update(ctx, deployment)
			if err != nil {
				return err
			}
		}
	}

	//回滚完成，更改scaler状态
	scaler.Status.Status = apiv1alpha1.RESTORED
	err := r.Status().Update(ctx, scaler)
	if err != nil {
		return err
	}

	return nil
}

func addAnnotation(ctx context.Context, scaler *apiv1alpha1.Scaler, r *ScalerReconciler) error {
	for _, deploy := range scaler.Spec.Deployments {
		deployment := &v1.Deployment{}
		err := r.Get(ctx, types.NamespacedName{
			Namespace: deploy.Namespace,
			Name:      deploy.Name,
		}, deployment)
		if err != nil {
			return err
		}

		//开始记录deployment信息
		if *deployment.Spec.Replicas != scaler.Spec.Replicas {
			logger.Info("add original state to originalDeployment map.")
			originalDeploymentInfo[deployment.Name] = apiv1alpha1.DeploymentInfo{
				Replicas:  *deployment.Spec.Replicas,
				Namespace: deployment.Namespace,
			}
		}
	}

	//将原始记录加入到scaler的annotation
	for name, info := range originalDeploymentInfo {
		deployInfo, err := json.Marshal(info)
		if err != nil {
			return err
		}
		annotationMap[name] = string(deployInfo)
	}
	scaler.ObjectMeta.Annotations = annotationMap
	err := r.Update(ctx, scaler)
	if err != nil {
		return err
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ScalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&apiv1alpha1.Scaler{}).
		Complete(r)
}
