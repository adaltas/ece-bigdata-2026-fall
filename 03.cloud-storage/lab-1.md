# Lab

Kubernetes provides mechanisms to persist data independently of the lifecycle of Pods. PersistentVolumes (PV), PersistentVolumeClaims (PVC), and StorageClasses define how persistent storage is provided and used by applications. This lab introduces these concepts through a practical example on Minikube.

## Covered Techniques

- Create a PersistentVolume
- Create a PersistentVolumeClaim
- Mount the PersistentVolume in the Pod

## Prerequisites

- A running Kubernetes cluster (minikube)
- Familiarity with Pods and basic `kubectl` commands

## Tutorial

Reference to the tutorial [here](https://kubernetes.io/docs/tutorials/configuration/configure-persistent-volume-storage/) and reproduce all of the steps.

[!NOTE]
The kubernetes resources will be created in the `default` namespace. If needed, create a new working namespace and, throughout the tutorial, add it to the created manifest files. To do this, see the [02.containerisation-et-kubernetes module's lab](../02.containerisation-et-kubernetes).
