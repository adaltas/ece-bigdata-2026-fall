---
duration: 0.5h
category:
  - name: LAB
components:
  - name: HOP
  - name: SPARK
  - name: S3
platforms:
  - name: LINUX
resources:
  - title: Apache Polaris documentation
    url: https://polaris.apache.org/
  - title: Polaris access control (securable objects, privileges, roles)
    url: https://polaris.apache.org/releases/1.8.0/managing-security/access-control/
  - title: Apache Iceberg, Spark configuration
    url: https://iceberg.apache.org/docs/latest/spark-configuration
  - title: Apache Iceberg, REST catalog specification
    url: https://iceberg.apache.org/rest-catalog-spec/
revisions:
  - date: 2026-10-29
    comment: Initial draft
    author: mori@adaltas.com
  - date: 2026-11-09
    comment: Deploy Hop Web in Adaltas cloud
    author: mori@adaltas.com
tags:
  - name: TUTORIAL
---

# Lab: deploy Apache Hop Web (k8s)

This lab is a prerequisite for the [Apache Hop lab](./lab-2.md). It describes how to deploy Apache Hop on the Adaltas Kubernetes platform.

## Prerequisites

- The `vscode-pyspark` Onyxia service with limits of `cpu: 2000m`, `memory 2000Mi`.

```bash
STATEFULSET_NAME="$(kubectl get statefulsets.apps -o=jsonpath='{.items[0].metadata.name}' -l app.kubernetes.io/name=vscode-pyspark)"
kubectl patch statefulset $STATEFULSET_NAME \
  -n $KUBERNETES_NAMESPACE \
  --type='strategic' \
  -p='{
    "spec": {
      "template": {
        "spec": {
          "containers": [{
            "name": "vscode-pyspark",
            "resources": {
              "requests": {
                "cpu": "50m",
                "memory": "200Mi"
              },
              "limits": {
                "cpu": "2000m"
                ,"memory": "2000Mi"
              }
            }
          }]
        }
      }
    }
  }'
kubectl rollout status statefulset/$STATEFULSET_NAME -n $KUBERNETES_NAMESPACE
```

## Hop configuration files

Two configuration files are created before the deployment.

```bash
GIT_REPO_NAME="<git-repo-name>"
cd "/home/onyxia/work/$GIT_REPO_NAME"
mkdir lab_hop
```

### Hop server configuration file

A Hop server file is created for https connection.

> [!NOTE]
> The Ingress terminates HTTPS and forwards plain HTTP to the pod, so Tomcat (the server behind Hop Web) does not know the original scheme. Hop Web then builds `http://` addresses on an `https://` page, the browser blocks them, and the page hangs when you open a file. The `server.xml` mounted from the `hop-web-server-xml` ConfigMap adds the `RemoteIpValve`, which reads the `X-Forwarded-Proto` header sent by nginx and restores the HTTPS scheme.

```bash
cat <<'XML'> ./lab_hop/server.xml
<?xml version="1.0" encoding="UTF-8"?>
<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<!-- Note:  A "Server" is not itself a "Container", so you may not
     define subcomponents such as "Valves" at this level.
     Documentation at /docs/config/server.html
 -->
<Server port="8005" shutdown="SHUTDOWN">
  <Listener className="org.apache.catalina.startup.VersionLoggerListener" />
  <!-- Security listener. Documentation at /docs/config/listeners.html
  <Listener className="org.apache.catalina.security.SecurityListener" />
  -->
  <!-- OpenSSL support using Tomcat Native -->
  <Listener className="org.apache.catalina.core.AprLifecycleListener" />
  <!-- OpenSSL support using FFM API from Java 22 -->
  <!-- <Listener className="org.apache.catalina.core.OpenSSLLifecycleListener" /> -->
  <!-- Prevent memory leaks due to use of particular java/javax APIs-->
  <Listener className="org.apache.catalina.core.JreMemoryLeakPreventionListener" />
  <Listener className="org.apache.catalina.mbeans.GlobalResourcesLifecycleListener" />
  <Listener className="org.apache.catalina.core.ThreadLocalLeakPreventionListener" />

  <!-- Global JNDI resources
       Documentation at /docs/jndi-resources-howto.html
  -->
  <GlobalNamingResources>
    <!-- Editable user database that can also be used by
         UserDatabaseRealm to authenticate users
    -->
    <Resource name="UserDatabase" auth="Container"
              type="org.apache.catalina.UserDatabase"
              description="User database that can be updated and saved"
              factory="org.apache.catalina.users.MemoryUserDatabaseFactory"
              pathname="conf/tomcat-users.xml" />
  </GlobalNamingResources>

  <!-- A "Service" is a collection of one or more "Connectors" that share
       a single "Container" Note:  A "Service" is not itself a "Container",
       so you may not define subcomponents such as "Valves" at this level.
       Documentation at /docs/config/service.html
   -->
  <Service name="Catalina">

    <!--The connectors can use a shared executor, you can define one or more named thread pools-->
    <!--
    <Executor name="tomcatThreadPool" namePrefix="catalina-exec-"
        maxThreads="150" minSpareThreads="4"/>
    -->


    <!-- A "Connector" represents an endpoint by which requests are received
         and responses are returned. Documentation at :
         HTTP Connector: /docs/config/http.html
         AJP  Connector: /docs/config/ajp.html
         Define a non-SSL/TLS HTTP/1.1 Connector on port 8080
    -->
    <Connector port="8080" protocol="HTTP/1.1"
               connectionTimeout="20000"
               redirectPort="8443"
               maxParameterCount="1000"
               />
    <!-- A "Connector" using the shared thread pool-->
    <!--
    <Connector executor="tomcatThreadPool"
               port="8080" protocol="HTTP/1.1"
               connectionTimeout="20000"
               redirectPort="8443"
               maxParameterCount="1000"
               />
    -->
    <!-- Define an SSL/TLS HTTP/1.1 Connector on port 8443 with HTTP/2
         This connector uses the NIO implementation. The default
         SSLImplementation will depend on the presence of the APR/native
         library and the useOpenSSL attribute of the AprLifecycleListener.
         Either JSSE or OpenSSL style configuration may be used regardless of
         the SSLImplementation selected. JSSE style configuration is used below.
    -->
    <!--
    <Connector port="8443" protocol="org.apache.coyote.http11.Http11NioProtocol"
               maxThreads="150" SSLEnabled="true"
               maxParameterCount="1000"
               >
        <UpgradeProtocol className="org.apache.coyote.http2.Http2Protocol" />
        <SSLHostConfig>
            <Certificate certificateKeystoreFile="conf/localhost-rsa.jks"
                         certificateKeystorePassword="changeit" type="RSA" />
        </SSLHostConfig>
    </Connector>
    -->

    <!-- Define an AJP 1.3 Connector on port 8009 -->
    <!--
    <Connector protocol="AJP/1.3"
               address="::1"
               port="8009"
               redirectPort="8443"
               maxParameterCount="1000"
               />
    -->

    <!-- An Engine represents the entry point (within Catalina) that processes
         every request.  The Engine implementation for Tomcat stand alone
         analyzes the HTTP headers included with the request, and passes them
         on to the appropriate Host (virtual host).
         Documentation at /docs/config/engine.html -->

    <!-- You should set jvmRoute to support load-balancing via AJP ie :
    <Engine name="Catalina" defaultHost="localhost" jvmRoute="jvm1">
    -->
    <Engine name="Catalina" defaultHost="localhost">

      <!--For clustering, please take a look at documentation at:
          /docs/cluster-howto.html  (simple how to)
          /docs/config/cluster.html (reference documentation) -->
      <!--
      <Cluster className="org.apache.catalina.ha.tcp.SimpleTcpCluster"/>
      -->

      <!-- Use the LockOutRealm to prevent attempts to guess user passwords
           via a brute-force attack -->
      <Realm className="org.apache.catalina.realm.LockOutRealm">
        <!-- This Realm uses the UserDatabase configured in the global JNDI
             resources under the key "UserDatabase".  Any edits
             that are performed against this UserDatabase are immediately
             available for use by the Realm.  -->
        <Realm className="org.apache.catalina.realm.UserDatabaseRealm"
               resourceName="UserDatabase"/>
      </Realm>

      <Host name="localhost"  appBase="webapps"
            unpackWARs="true" autoDeploy="true">

        <!-- SingleSignOn valve, share authentication between web applications
             Documentation at: /docs/config/valve.html -->
        <!--
        <Valve className="org.apache.catalina.authenticator.SingleSignOn" />
        -->
        <Valve className="org.apache.catalina.valves.RemoteIpValve" protocolHeader="X-Forwarded-Proto" />
        <!-- Access log processes all example.
             Documentation at: /docs/config/valve.html
             Note: The pattern used is equivalent to using pattern="common" -->
        <Valve className="org.apache.catalina.valves.AccessLogValve" directory="logs"
               prefix="localhost_access_log" suffix=".txt"
               pattern="%h %l %u %t &quot;%r&quot; %s %b" />

      </Host>
    </Engine>
  </Service>
</Server>
XML
kubectl create configmap hop-web-server-xml --from-file=./lab_hop/server.xml
```

### Hop deployment files

Create a manifest file that deploys Hop Web in your namespace:

```bash
cat <<YAML >./lab_hop/hop-web.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: hop-web-data
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hop-web
  labels:
    app.kubernetes.io/name: hop-web
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/name: hop-web
  template:
    metadata:
      labels:
        app.kubernetes.io/name: hop-web
    spec:
      securityContext:
        fsGroup: 501
      containers:
        - name: hop
          image: docker.io/apache/hop-web:2.19.0
          imagePullPolicy: IfNotPresent
          ports:
            - name: http
              containerPort: 8080
              protocol: TCP
          resources:
            requests:
              cpu: 100m
              memory: 512Mi
            limits:
              cpu: "2"
              memory: 2000Mi
          envFrom:
            - secretRef:
                name: hop-s3
          volumeMounts:
            - { name: server-xml, mountPath: /usr/local/tomcat/conf/server.xml, subPath: server.xml }
            - { name: data, mountPath: /home/hop/hop-project }
      volumes:
        - name: server-xml
          configMap: { name: hop-web-server-xml }
        - name: data
          persistentVolumeClaim: { claimName: hop-web-data }
---
apiVersion: v1
kind: Service
metadata:
  name: hop-web
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: hop-web
  ports:
    - name: http
      port: 8080
      targetPort: http
      protocol: TCP
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: hop-web-${KUBERNETES_NAMESPACE}
  annotations:
    nginx.ingress.kubernetes.io/ssl-redirect: "false"
spec:
  ingressClassName: nginx
  rules:
    - host: hop-web-${KUBERNETES_NAMESPACE}.lab.adm.adaltas.cloud
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: hop-web
                port:
                  number: 8080
  tls:
  - hosts:
    - hop-web-${KUBERNETES_NAMESPACE}.lab.adm.adaltas.cloud
YAML
```

## Environment

First check the S3 connectivity. If the `AWS_*` environment variables are missing or outdated, refresh the `default` profile configuration and export them to the current shell. Refers to lab of [Object storage with S3](../03.object-storage/lab-2-s3.md#environment).

```bash
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"
#> 2026-09-14 11:02:10     331568 orders.csv
#> 2026-09-14 11:02:09       7351 users.csv
```

The `Job` does not see the environment of the VSCode service. Create a `Secret` from the temporary credentials, and one with the URL of the repository. The credentials are temporary. If they expire, rerun the following command to update.

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
kubectl create secret generic hop-s3 \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  --from-literal=AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
  --from-literal=AWS_S3_ENDPOINT="$AWS_S3_ENDPOINT" \
  --from-literal=LAB_BUCKET_NAME="$LAB_BUCKET_NAME" \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Deployment

```bash
kubectl apply -f ./lab_hop/hop-web.yaml
```

Access the Hop web.

```bash
HOP_WEB_URL="http://hop-web-$KUBERNETES_NAMESPACE.lab.adm.adaltas.cloud"
echo $HOP_WEB_URL
```

## Update Hop pods

If the AWS credentials are outdated, update the secret by rerunning the above `kubectl create` command again and relaunch the Hop web pod.

```bash
kubectl scale deployment/hop-web --replicas=0
kubectl scale deployment/hop-web --replicas=1
kubectl rollout status deployment/hop-web
#> deployment "hop-web" successfully rolled out
```

If the `hop-project` has already been created in the pods (that is, you’ve completed the Hop lab or are partway through it), you need to register the project again in Hop Web. See the [Project registration section](./lab-2.md).

## Teardown

The aim of this section is to show how to remove Hop when it is no longer needed. For the next module, keep it.

```bash
kubectl delete -f ./lab_hop/hop-web.yaml
kubectl delete configmap hop-web-server-xml
kubectl delete secret hop-s3
```
