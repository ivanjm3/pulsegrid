# main.tf — Pulsegrid infrastructure, sized for the AWS free tier by default
# (see variables.tf for what's free-tier-eligible and what isn't).
#
# NOT free-tier eligible, unavoidably, regardless of sizing:
#   - EKS control plane: flat $0.10/hr (~$73/mo). AWS has never offered a
#     free EKS control plane; there's no smaller "free" tier of EKS itself.
#     This is the one line item in this file cost minimization can't remove
#     while still satisfying design.md's "Kubernetes cluster" requirement.
#   - Kafka: per design.md's own task-30 note ("no MSK, Kafka in-cluster"),
#     this file does NOT provision MSK. Run Kafka in-cluster (e.g. Strimzi,
#     or Bitnami's chart) — that cost shows up as EC2/EBS on the node group,
#     not as a separate line item here.
# Everything else (node instance type/count, RDS class/storage/Multi-AZ, NAT
# gateway) is parameterized in variables.tf with free-tier-eligible defaults.

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  # Remote state in S3 with encryption + a DynamoDB lock table, per
  # requirements.md #14.4. Both the state bucket and lock table are cheap:
  # a lock table's few-KB items and a small state file sit comfortably
  # inside the S3 (5GB) and DynamoDB (25GB, 25 WCU/RCU) free tiers.
  # Bootstrap these two resources by hand (or via a separate one-time
  # `terraform apply` with this block commented out) before pointing this
  # block at them — a backend block cannot reference resources this same
  # config creates.
  backend "s3" {
    bucket         = "pulsegrid-terraform-state"
    key            = "dev/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "pulsegrid-terraform-locks"
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}
data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  common_tags = {
    Project     = "pulsegrid"
    Environment = var.environment
    ManagedBy   = "terraform"
    CostCenter  = "pulsegrid-${var.environment}"
  }
  azs = slice(data.aws_availability_zones.available.names, 0, 2) # 2 AZs, not 3 — task 30's "minimal footprint" note
}

# ---------------------------------------------------------------------------
# Networking — 2 AZs, single (optional) NAT gateway. See variables.tf's
# enable_nat_gateway for the cost tradeoff this makes by default.
# ---------------------------------------------------------------------------

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(local.common_tags, { Name = "pulsegrid-vpc-${var.environment}" })
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = merge(local.common_tags, { Name = "pulsegrid-igw-${var.environment}" })
}

# Node + RDS subnets. Whether these get a route to the internet gateway
# directly (no NAT) or via NAT is decided by the route table below, not by
# anything in the subnet resource itself.
resource "aws_subnet" "nodes" {
  count                   = length(local.azs)
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.${count.index + 1}.0/24"
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = !var.enable_nat_gateway

  tags = merge(local.common_tags, {
    Name                     = "pulsegrid-nodes-${var.environment}-${count.index + 1}"
    "kubernetes.io/role/elb" = "1"
  })
}

resource "aws_eip" "nat" {
  count  = var.enable_nat_gateway ? 1 : 0
  domain = "vpc"
  tags   = merge(local.common_tags, { Name = "pulsegrid-nat-eip-${var.environment}" })
}

resource "aws_nat_gateway" "main" {
  count         = var.enable_nat_gateway ? 1 : 0
  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.nodes[0].id
  tags          = merge(local.common_tags, { Name = "pulsegrid-nat-${var.environment}" })
}

resource "aws_route_table" "nodes" {
  vpc_id = aws_vpc.main.id
  tags   = merge(local.common_tags, { Name = "pulsegrid-nodes-rt-${var.environment}" })

  dynamic "route" {
    for_each = var.enable_nat_gateway ? [] : [1]
    content {
      cidr_block = "0.0.0.0/0"
      gateway_id = aws_internet_gateway.main.id
    }
  }

  dynamic "route" {
    for_each = var.enable_nat_gateway ? [1] : []
    content {
      cidr_block     = "0.0.0.0/0"
      nat_gateway_id = aws_nat_gateway.main[0].id
    }
  }
}

resource "aws_route_table_association" "nodes" {
  count          = length(aws_subnet.nodes)
  subnet_id      = aws_subnet.nodes[count.index].id
  route_table_id = aws_route_table.nodes.id
}

resource "aws_security_group" "eks_cluster" {
  name   = "pulsegrid-eks-cluster-sg-${var.environment}"
  vpc_id = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

resource "aws_security_group" "rds" {
  name   = "pulsegrid-rds-sg-${var.environment}"
  vpc_id = aws_vpc.main.id

  ingress {
    description     = "Postgres from EKS nodes/cluster only"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.eks_cluster.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# EKS
# ---------------------------------------------------------------------------

resource "aws_iam_role" "eks_cluster" {
  name = "pulsegrid-eks-cluster-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.eks_cluster.name
}

resource "aws_eks_cluster" "pulsegrid" {
  name     = "pulsegrid-${var.environment}"
  role_arn = aws_iam_role.eks_cluster.arn
  version  = var.eks_cluster_version

  vpc_config {
    subnet_ids              = aws_subnet.nodes[*].id
    security_group_ids      = [aws_security_group.eks_cluster.id]
    endpoint_private_access = true
    endpoint_public_access  = true
  }

  depends_on = [aws_iam_role_policy_attachment.eks_cluster_policy]
  tags       = local.common_tags
}

# IRSA — lets pulsegrid-api/pulsegrid-worker ServiceAccounts assume scoped
# IAM roles for S3 access instead of static keys (kube/rbac.yaml's
# eks.amazonaws.com/role-arn annotations point at these two roles' ARNs).
data "tls_certificate" "eks_oidc" {
  url = aws_eks_cluster.pulsegrid.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  url             = aws_eks_cluster.pulsegrid.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks_oidc.certificates[0].sha1_fingerprint]
  tags            = local.common_tags
}

locals {
  oidc_provider_url = replace(aws_iam_openid_connect_provider.eks.url, "https://", "")
}

resource "aws_iam_role" "node" {
  name = "pulsegrid-node-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "node_worker_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.node.name
}

resource "aws_iam_role_policy_attachment" "node_cni_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.node.name
}

resource "aws_iam_role_policy_attachment" "node_ecr_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.node.name
}

resource "aws_eks_node_group" "pulsegrid" {
  cluster_name    = aws_eks_cluster.pulsegrid.name
  node_group_name = "pulsegrid-${var.environment}"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = aws_subnet.nodes[*].id

  scaling_config {
    min_size     = var.min_nodes
    max_size     = var.max_nodes
    desired_size = var.desired_nodes
  }

  instance_types = [var.node_instance_type]
  disk_size      = 20 # gp2/gp3 root volume; EBS free tier covers 30GB-month, 20GB leaves headroom for RDS/backups sharing the account-wide allowance

  depends_on = [
    aws_iam_role_policy_attachment.node_worker_policy,
    aws_iam_role_policy_attachment.node_cni_policy,
    aws_iam_role_policy_attachment.node_ecr_policy,
  ]

  tags = merge(local.common_tags, { Name = "pulsegrid-node-group-${var.environment}" })
}

# ---------------------------------------------------------------------------
# S3 — pulsegrid-source / pulsegrid-output (design.md's bucket structure)
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "source" {
  bucket = "pulsegrid-source-${var.environment}-${data.aws_caller_identity.current.account_id}"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "output" {
  bucket = "pulsegrid-output-${var.environment}-${data.aws_caller_identity.current.account_id}"
  tags   = local.common_tags
}

# Versioning is a hard requirement (requirements.md #7.5), not a cost add-on
# by itself — cost only grows if noncurrent versions pile up, which the
# lifecycle rules below bound.
resource "aws_s3_bucket_versioning" "source" {
  bucket = aws_s3_bucket.source.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_versioning" "output" {
  bucket = aws_s3_bucket.output.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "source" {
  bucket = aws_s3_bucket.source.id
  rule {
    id     = "delete-after-30-days"
    status = "Enabled"
    filter {}
    expiration { days = 30 }
    noncurrent_version_expiration { noncurrent_days = 7 }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "output" {
  bucket = aws_s3_bucket.output.id
  rule {
    id     = "glacier-then-delete"
    status = "Enabled"
    filter {}
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    expiration { days = 365 }
    noncurrent_version_expiration { noncurrent_days = 30 }
  }
}

resource "aws_iam_policy" "s3_access" {
  name = "pulsegrid-s3-access-${var.environment}"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:PutObjectTagging", "s3:GetObjectTagging"]
        Resource = [
          "${aws_s3_bucket.source.arn}/*",
          "${aws_s3_bucket.output.arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.source.arn, aws_s3_bucket.output.arn]
      },
    ]
  })
}

# One IRSA role per ServiceAccount (api, worker), both scoped to the same S3
# policy — api only reads/writes the source bucket and worker reads source +
# writes output in practice, but pkg/storage doesn't split S3 permissions by
# service today, so splitting the IAM policy narrower than the code's actual
# access pattern would just be unenforceable-in-practice granularity.
resource "aws_iam_role" "api" {
  name = "pulsegrid-api-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRoleWithWebIdentity"
      Principal = { Federated = aws_iam_openid_connect_provider.eks.arn }
      Condition = {
        StringEquals = {
          "${local.oidc_provider_url}:sub" = "system:serviceaccount:pulsegrid:pulsegrid-api"
          "${local.oidc_provider_url}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "api_s3" {
  policy_arn = aws_iam_policy.s3_access.arn
  role       = aws_iam_role.api.name
}

resource "aws_iam_role" "worker" {
  name = "pulsegrid-worker-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRoleWithWebIdentity"
      Principal = { Federated = aws_iam_openid_connect_provider.eks.arn }
      Condition = {
        StringEquals = {
          "${local.oidc_provider_url}:sub" = "system:serviceaccount:pulsegrid:pulsegrid-worker"
          "${local.oidc_provider_url}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "worker_s3" {
  policy_arn = aws_iam_policy.s3_access.arn
  role       = aws_iam_role.worker.name
}

# ---------------------------------------------------------------------------
# RDS Postgres — db.t3.micro, single-AZ, 20GB by default (free-tier caps)
# ---------------------------------------------------------------------------

resource "aws_db_subnet_group" "pulsegrid" {
  name       = "pulsegrid-${var.environment}"
  subnet_ids = aws_subnet.nodes[*].id
  tags       = local.common_tags
}

resource "aws_db_instance" "pulsegrid" {
  identifier     = "pulsegrid-${var.environment}"
  engine         = "postgres"
  engine_version = "15.7"
  instance_class = var.db_instance_class

  allocated_storage = var.db_allocated_storage_gb
  storage_type      = "gp2" # gp2, not gp3 — gp2 is what the RDS free tier covers

  db_name  = "pulsegrid"
  username = "postgres"
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.pulsegrid.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  multi_az            = var.db_multi_az
  publicly_accessible = false

  skip_final_snapshot       = var.environment == "dev"
  final_snapshot_identifier = var.environment == "dev" ? null : "pulsegrid-${var.environment}-final-${formatdate("YYYY-MM-DD-hhmm", timestamp())}"

  backup_retention_period = var.db_backup_retention_days

  tags = merge(local.common_tags, { Name = "pulsegrid-db-${var.environment}" })
}
