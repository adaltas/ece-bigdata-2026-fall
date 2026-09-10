# Lab

Basic manipulation with Kubernetes commands.

## Objectives

1. Install minikube
2. Learn to use `kubectl` commands
3. Learn to expose a Kubernetes service to the outside
4. Learn to scale up and down a Kubernetes deployment
5. Run a multiple pod application in Kubernetes
6. Deploy an app using manifest yaml files

## 1. Install minikube

[Install minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/) following the instructions depending on your OS (you may skip step 4 "Deploy applications").

Start minikube with:

```bash
minikube start
```

Verify global status of cluster:

```bash
minikube status
kubectl get nodes
kubectl describe node <node-name>
```

Activate metrics server and show the resources consumption.

```bash
minikube addons enable metrics-server
```

Observe and explain what the command below does.

```bash
kubectl top pods -A --sort-by cpu --sum=true
```

Explore `kube-system` namespace and understand the role of each component by reading the Kubernetes components overview [here](https://kubernetes.io/docs/concepts/overview/components/).

## 2. Learn to use `kubectl` commands

1. Open a terminal

2. Run a `deployment` with one `pod` with the following command:

   ```bash
   kubectl create deployment kubernetes-bootcamp --image=gcr.io/google-samples/kubernetes-bootcamp:v1
   ```

   `gcr.io/google-samples/kubernetes-bootcamp:v1` is a Docker image of a basic Node.js web application.

   **NOTE!** You may need to install `kubectl` following the instructions [here](https://kubernetes.io/docs/tasks/tools/install-kubectl/). This should not be strictly necessary and you may substitute `kubectl` with `minikube kubectl` if preferred.

3. List all the running pods with:

   ```bash
   kubectl get pods
   ```

   Wait until Pod readiness reaches 1/1 and save the Pod name in environment variable `$POD_NAME`.

4. Display the pod logs and get more info:

   ```bash
   kubectl logs $POD_NAME
   kubectl describe pod $POD_NAME
   ```

5. Run a command inside the pod with:

   ```bash
   kubectl exec $POD_NAME -- cat /etc/os-release
   ```

6. Open a shell inside the pod with:

   ```bash
   kubectl exec -it $POD_NAME -- bash
   ```

7. List the content of the directory you are in and try to find the JavaScript source code file.

8. Make sure that the web app is responding inside the container by querying it with `curl`.

   > **Hint.** The port on which the app responds is defined in the `/server.js` JavaScript file

9. Are you able to query the web app outside of the pod (from your local machine)?

## 3. Learn to expose a Kubernetes service outside the minikube cluster

1. Expose the deployment you created in the first part of the lab with:

   ```bash
   kubectl expose deployments/$DEPLOYMENT_NAME --type="NodePort" --port $PORT_NUMBER
   ```

   > **Hint.** You need to replace `$DEPLOYMENT_NAME` with the actual name of the `deployment` as well as `$PORT_NUMBER`

2. Find out which port the service has been attached with:

   ```bash
   kubectl get services
   ```

3. Get the IP of your minikube VM with:
   ```bash
   minikube ip
   ```
4. Using the answers of questions 2 and 3, open your web browser and try to reach the web app.

> **Note!** If you are using Docker driver in minikube, you must create a tunnel to the cluster node (that is running as a Docker container). Run the command (replace `$SERVICE_NAME` with your service name):

```bash
minikube service $SERVICE_NAME
```

## 4. Learn to scale up and down a Kubernetes deployment

1. Scale up your deployment to a total number of 5 pods with:

```bash
kubectl scale deployments/kubernetes-bootcamp --replicas=5
```

2. Make sure that you have 5 pods running using one of the commands we have seen in part 2 of the lab. Which command did you use?

3. Open the exposed service through your web browser again.
   Force refresh a couple of times using `CTRL+F5`
   What is happening? Why?
4. Scale down again your deployment to 2 pods and confirm the other 3 are not running anymore.

## 5. Run a multiple pod application in Kubernetes

1. Prepare to hit `CTRL+F5` on your browser multiple times right after launching the following command:
2. Update the Docker image used by the `deployment` with:
   ```bash
   kubectl set image deployments/$DEPLOYMENT_NAME kubernetes-bootcamp=jocatalin/kubernetes-bootcamp:v2
   ```
3. What happened to the web page?
4. Update the Docker image used by the `deployment` again by setting the image to `jocatalin/kubernetes-bootcamp:v3`
5. List all the running pods, what is happening here?
6. Cancel the previous operation by running:

   ```bash
   kubectl rollout undo deployments/kubernetes-bootcamp
   ```

7. Roll back the service to the image we first chose in part 2 of the lab.

## 6. Deploy an app using manifest yaml files

1. Clean up what you did in the previous part with:

   ```bash
   kubectl delete service $SERVICE_NAME
   kubectl delete deployment $DEPLOYMENT_NAME
   ```

2. Using the [deployment documentation](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/), fill out the blank (`TO COMPLETE #1`) in [`./lab/deployment.yaml`](./lab/deployment.yaml) to define a deployment based on the one we ran in part 2.

3. Once you completed the file, run:

   ```bash
   kubectl apply -f deployment.yaml
   ```

   Are the pods running?

4. Using the [service documentation](https://kubernetes.io/docs/concepts/services-networking/service/), fill out the blank in [`./lab/service.yaml`](./lab/service.yaml)

5. Once you completed the file, run:

   ```bash
   kubectl apply -f service.yaml
   ```

   Can you access the service through your web browser?

6. Fill out `TO COMPLETE #2` inside [`./lab/deployment.yaml`](./lab/deployment.yaml) to create 3 replicas of your app.

7. Once you completed the file, run:

   ```bash
   kubectl apply -f deployment.yaml
   ```

   Force refresh on the browser a couple of times. Are you hitting different replicas?

8. Once completed, clean up the cluster as before and stop minikube.

## 7. Teardown

Remove every workloads deployed in this lab.

```bash
kubectl delete -f service.yaml
kubectl delete -f deployment.yaml
```
