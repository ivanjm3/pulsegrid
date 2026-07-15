# AWS Architecture Review — PulseGrid

Audit date: 2026-07-14

## 1. Current Architecture

PulseGrid = video transcoding platform. Two services (`api`, `worker`) run as containers
on **EKS**, talk to **RDS Postgres** for state, **S3** for source/output video, and an
**in-cluster Kafka** (3 brokers, StatefulSet in `kube/`) for job queue. Terraform
provisions VPC + EKS + RDS + S3 + IAM + security groups. No ALB/CloudFront/Route53/
Lambda/ECS/API Gateway/SQS/SNS/EventBridge/Secrets Manager/KMS/CloudWatch-SDK found in
repo — those are NOT in use currently (good, already lean vs. the audit checklist).

Static AWS IAM keys (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`) sit in a plain k8s
Secret (`kube/secrets.yaml`) instead of IRSA, despite the EKS OIDC issuer already being
output by Terraform (`eks_cluster_oidc_issuer`) — IRSA is set up for nothing.

## 2. Resource Inventory

| Resource | Where | Purpose |
|---|---|---|
| VPC + IGW + 2 public/private subnets | terraform/main.tf | Network for EKS + RDS |
| NAT Gateway (1x) | terraform/main.tf | Egress for private-subnet nodes |
| Security Groups (eks_cluster, eks_nodes, rds) | terraform/main.tf | Network ACLs |
| IAM roles (eks_cluster, eks_nodes) + policy attachments | terraform/main.tf | EKS control plane / node permissions |
| IAM inline policy `eks_nodes_s3` | terraform/main.tf | Node S3 read/write to source+output buckets |
| EKS cluster + 2 node groups (api t3.small, worker t3.large) | terraform/main.tf | Runs `api` and `worker` containers |
| S3 buckets: source, output | terraform/main.tf | Video upload / transcoded storage, lifecycle to Glacier |
| RDS Postgres (db.t4g.micro, single-AZ) | terraform/main.tf | Job/user state |
| S3 bucket + DynamoDB table (state-bootstrap) | terraform/state-bootstrap/main.tf | Terraform remote state/lock (one-time, not app runtime) |
| Static IAM user access keys in k8s Secret | kube/secrets.yaml | App auth to S3 (should be IRSA) |
| Kafka (self-hosted, 3 brokers) | kube/ manifests | Job queue — NOT an AWS service, runs on EKS nodes you're already paying for |

No ECS, Lambda, ECR (image registry unspecified — presumably Docker Hub or unmanaged),
ALB/NLB, API Gateway, EventBridge, SQS, SNS, CloudFront, Route53, Secrets Manager, KMS,
or CloudWatch SDK calls found.

## 3. Required Resources

- **S3** (source + output buckets) — durable object storage, no cheaper AWS-native
  substitute for this workload.
- **RDS Postgres** or equivalent managed/self-hosted Postgres — needed for durable
  relational state.
- **Compute** to run `api` + `worker` containers — some form required; EKS is one option,
  not the only one (see §6).
- **VPC + subnets + security groups** — mandatory scaffolding wherever compute + RDS
  live, minimal cost itself (SGs free, VPC free).
- **IAM roles** — required for any AWS access; free.

## 4. Optional Resources

- **NAT Gateway** — only needed if nodes sit in private subnets. Terraform already has
  `enable_nat_gateway` toggle acknowledging this (~$32/mo + data processing). Can disable
  and put nodes in public subnets with tight SGs, or replace with a NAT instance
  (~$3-8/mo t3.nano) if egress still needed.
- **RDS Multi-AZ** — currently off (`db_multi_az = false`), correctly optional for
  non-prod. Keep off unless uptime SLA demands it.
- **RDS Performance Insights** — off by default already, correctly optional.
- **EKS control-plane CloudWatch logs** — off by default already, correctly optional.
- **S3 versioning** — off by default already, correctly optional.

## 5. Resources to Remove

- **Static AWS IAM access keys in `kube/secrets.yaml`** — remove. Replace with IRSA
  (OIDC provider + IAM role trust policy scoped to the `pulsegrid` service account);
  the cluster already emits the OIDC issuer needed for this. Removes credential-leak
  risk and a rotation burden, no cost difference.
- Nothing else in the current inventory is dead weight — this is already a fairly
  minimal AWS footprint (no ALB, no API Gateway, no Lambda sprawl, no SQS/SNS
  duplicating Kafka).

## 6. Resources to Replace

| Current | Replace with | Why | Cost impact | Complexity impact |
|---|---|---|---|---|
| **EKS** ($73/mo control plane + node costs) | **Plain EC2 instances (2-3) running Docker Compose / systemd units**, or **ECS on EC2** | For 2 services + self-hosted Kafka, Kubernetes is heavy operational overhead (control plane, node groups, RBAC, manifests) for workload that doesn't need pod-level orchestration elasticity. EKS control plane alone costs $73/mo before any nodes. | Saves ~$73/mo flat control-plane fee, plus removes need for 2 separate node groups (t3.small + t3.large minimums) | Large complexity reduction: no kubectl/helm/manifests, no EKS upgrade treadmill, no IRSA setup, no node-group autoscaling config |
| **Self-hosted 3-broker Kafka on EKS worker nodes** | **Amazon SQS** (if strict ordering not required) or keep Kafka but run 1 broker (dev) | 3-broker Kafka StatefulSet is expensive in ops complexity (ZooKeeper/KRaft, PVCs, broker rebalancing) for a single-queue transcoding-jobs use case. If you need ordering/replay/DLQ semantics Kafka already gives you, SQS + a DLQ replicates that with zero ops. If Kafka's throughput/fan-out features are actually used, keep it but run fewer brokers outside prod. | SQS: ~$0.40/million requests, likely <$5/mo at this scale vs. 3 extra worker-node-equivalents of Kafka compute | Large reduction: no broker/ZK management, no StatefulSet, DLQ built in |
| **NAT Gateway** | **NAT instance (t3.nano) or public-subnet nodes with restrictive SGs** | NAT Gateway is $32/mo + $0.045/GB processed regardless of traffic; a t3.nano NAT instance is ~$3/mo, or skip entirely if nodes can be public with locked-down SGs | Saves ~$25-30/mo | Slightly more ops (patch a NAT instance) or negligible (public subnet + SG) |
| **db.t4g.micro RDS** | Keep as-is (already minimal) — no cheaper managed Postgres exists at this tier; self-hosting Postgres on EC2 saves ~$13/mo but adds backup/patching ops that RDS's price is worth avoiding | RDS retained as required resource | n/a | n/a |

## 7. Estimated Monthly Cost

Current (approx, us-east-1, dev environment defaults, single NAT):

| Item | Est. $/mo |
|---|---|
| EKS control plane | $73 |
| EKS API node (1x t3.small on-demand) | ~$15 |
| EKS worker node (1x t3.large on-demand) | ~$60 |
| NAT Gateway (fixed) + light data processing | ~$35 |
| RDS db.t4g.micro, 20GB gp3, single-AZ | ~$13 |
| S3 (source+output, light usage, +Glacier transitions) | ~$5-15 (usage-dependent) |
| Terraform state S3 + DynamoDB (pay-per-request) | <$1 |
| **Total** | **~$200-210/mo** at minimum idle scale, before autoscaling worker nodes for transcoding load |

Minimal architecture (below) estimate: **~$60-90/mo** at equivalent idle scale —
mainly EC2 instances replacing EKS control plane + NAT Gateway.

## 8. Recommended Minimal Production Architecture

- **Compute**: 2 EC2 instances behind no LB (or 1 small Elastic IP-backed instance if
  traffic is light) — one running `api` (Docker Compose, systemd-managed), one running
  `worker` + Kafka (or drop Kafka for SQS). Use an Auto Scaling Group of 1-2 for the
  worker tier if transcoding load is spiky; skip ASG for api if traffic is steady.
- **Queue**: SQS + DLQ instead of self-hosted Kafka, unless multi-consumer replay is a
  hard requirement — cuts a full stateful cluster down to a managed queue.
- **Storage**: keep S3 source/output buckets exactly as configured (lifecycle rules
  already sensible).
- **Database**: keep RDS db.t4g.micro single-AZ (already minimal; don't self-host —
  backup/patch ops cost more than the ~$13/mo saved).
- **Networking**: VPC with public subnets only for these instance counts; drop NAT
  Gateway. If private subnets are mandated by policy, use a NAT instance not NAT Gateway.
- **Auth**: IAM instance profile on EC2 (replaces IRSA-on-EKS, same idea) scoped to the
  two S3 buckets + SQS queue — no more static keys in config.
- **Observability**: CloudWatch Agent on instances for basic metrics/logs (cheap,
  pay-per-use) instead of running Prometheus/Grafana stack — or keep the existing
  Prometheus/Grafana manifests (`kube/prometheus-rules.yaml`, `grafana-dashboard.json`)
  running as plain containers on the api instance if that tooling investment matters more
  than cost.

This preserves every existing capability (upload → queue → transcode → store → serve)
while removing the EKS control plane fee, the 3-broker Kafka cluster's operational
weight, and the NAT Gateway's fixed cost — the three biggest cost/complexity drivers
that aren't buying capabilities this workload uses.

## 9. Migration Steps

1. **Stop the bleeding first**: swap static IAM keys in `kube/secrets.yaml` for
   IRSA-scoped role — zero-downtime, do regardless of anything else below.
2. **Validate SQS fit**: confirm nothing in `pkg/` or `cmd/worker` depends on
   Kafka-specific semantics (consumer groups, replay, ordering across partitions) that
   SQS/DLQ can't replicate. If dependent, keep Kafka but shrink to 1 broker outside prod.
3. **Stand up EC2 target**: provision 2 EC2 instances (api, worker) with Docker installed,
   IAM instance profiles scoped to S3 (+SQS if migrated), same SGs the RDS SG already
   expects (swap `eks_nodes` SG reference for a new `app_instances` SG in the RDS SG rule).
4. **Deploy containers via Docker Compose or plain `docker run`** using the existing
   `Dockerfile.api` / `Dockerfile.worker` images — no manifest translation needed since
   these already build standalone images.
5. **Cut over traffic**: point DNS/clients at new instances, verify job flow end-to-end
   (upload → S3 → queue → transcode → output bucket → DB write).
6. **Decommission EKS**: delete node groups, then cluster, then associated IAM roles/SGs
   in Terraform (`terraform destroy -target=...` per resource, or rewrite `main.tf` to
   drop `aws_eks_*` and `aws_iam_role.eks_*` resources, keeping VPC/S3/RDS blocks).
7. **Drop NAT Gateway**: move remaining private-subnet resources (RDS) to reference new
   SGs; RDS itself doesn't need internet egress, so removing NAT after EKS is gone is
   usually safe — confirm no other resource needs outbound internet from private subnets.
8. **Re-point Terraform state**: no change needed, state-bootstrap resources are
   independent of this migration.
