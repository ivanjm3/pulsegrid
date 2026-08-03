variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "eks_cluster_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.29"
}

# t3.micro is AWS free-tier eligible (750 instance-hours/month for the first
# 12 months of a new account). It does NOT meet design.md's stated worker
# requirement (2 CPU / 4 GB request) — this default is for standing up the
# cluster and running the API server + light smoke-test load within the free
# tier, not for production transcoding throughput. Override for real load.
variable "node_instance_type" {
  description = "EC2 instance type for the EKS managed node group"
  type        = string
  default     = "t3.micro"
}

variable "min_nodes" {
  description = "Minimum number of worker nodes"
  type        = number
  default     = 1
}

variable "max_nodes" {
  description = "Maximum number of worker nodes"
  type        = number
  default     = 2
}

variable "desired_nodes" {
  description = "Desired number of worker nodes"
  type        = number
  default     = 1
}

# db.t3.micro is the free-tier-eligible RDS instance class (750 hours/month,
# first 12 months). Multi-AZ, larger classes, and >20GB storage all fall
# outside the free tier, so they are off by default here.
variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "db_allocated_storage_gb" {
  description = "RDS allocated storage in GB (free tier cap: 20GB)"
  type        = number
  default     = 20
}

variable "db_multi_az" {
  description = "Enable RDS Multi-AZ (NOT free-tier eligible — leave false for dev)"
  type        = bool
  default     = false
}

variable "db_backup_retention_days" {
  description = "RDS automated backup retention in days (0 disables backups, keeps storage/cost minimal for dev)"
  type        = number
  default     = 1
}

variable "db_password" {
  description = "Master password for the RDS instance (pass via TF_VAR_db_password or a tfvars file kept out of git — never commit this)"
  type        = string
  sensitive   = true
}

# A NAT Gateway costs ~$0.045/hr + data processing regardless of the free
# tier (AWS has never offered a free NAT Gateway). Defaulting this to false
# puts node subnets in public address space with a public IP instead of
# routing egress through a NAT — the standard "private nodes" pattern from
# design.md costs real money either way, so for the bare-minimum-cost dev
# footprint this trades that isolation for a straight $0/mo networking bill.
# Set true for anything beyond a personal/dev cluster.
variable "enable_nat_gateway" {
  description = "Route node subnet egress through a NAT Gateway instead of a public IP (adds hourly + data cost)"
  type        = bool
  default     = false
}
