# -----------------------------------------------------------------------------
# Pulsegrid Infrastructure Variables (minimal defaults for small deployments)
# -----------------------------------------------------------------------------

variable "region" {
  description = "AWS region for all resources"
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

variable "project_name" {
  description = "Project name used for resource naming and tagging"
  type        = string
  default     = "pulsegrid"
}

# --- VPC ---

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "Availability zones (first az_count entries are used)"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b"]
}

variable "az_count" {
  description = "Number of AZs to use (minimum 2 for RDS subnet group)"
  type        = number
  default     = 2

  validation {
    condition     = var.az_count >= 2 && var.az_count <= 3
    error_message = "az_count must be between 2 and 3."
  }
}

variable "enable_nat_gateway" {
  description = "Create a single NAT gateway for private subnet egress (disable to save ~$32/mo; nodes must use public subnets)"
  type        = bool
  default     = true
}

# --- EKS ---

variable "eks_cluster_version" {
  description = "Kubernetes version for EKS cluster"
  type        = string
  default     = "1.29"
}

variable "enable_eks_control_plane_logs" {
  description = "Enable EKS control plane CloudWatch logs (adds cost)"
  type        = bool
  default     = false
}

variable "api_node_instance_types" {
  description = "Instance types for API server node group"
  type        = list(string)
  default     = ["t3.small"]
}

variable "api_node_min_size" {
  description = "Minimum number of nodes in API node group"
  type        = number
  default     = 1
}

variable "api_node_max_size" {
  description = "Maximum number of nodes in API node group"
  type        = number
  default     = 3
}

variable "api_node_desired_size" {
  description = "Desired number of nodes in API node group"
  type        = number
  default     = 1
}

variable "worker_node_instance_types" {
  description = "Instance types for worker node group (ffmpeg workloads)"
  type        = list(string)
  default     = ["t3.large"]
}

variable "worker_node_capacity_type" {
  description = "EC2 capacity type for worker nodes (ON_DEMAND or SPOT)"
  type        = string
  default     = "ON_DEMAND"

  validation {
    condition     = contains(["ON_DEMAND", "SPOT"], var.worker_node_capacity_type)
    error_message = "worker_node_capacity_type must be ON_DEMAND or SPOT."
  }
}

variable "worker_node_min_size" {
  description = "Minimum number of nodes in worker node group"
  type        = number
  default     = 0
}

variable "worker_node_max_size" {
  description = "Maximum number of nodes in worker node group"
  type        = number
  default     = 5
}

variable "worker_node_desired_size" {
  description = "Desired number of nodes in worker node group"
  type        = number
  default     = 1
}

# --- RDS ---

variable "db_instance_class" {
  description = "RDS instance class for Postgres"
  type        = string
  default     = "db.t4g.micro"
}

variable "db_allocated_storage" {
  description = "Allocated storage in GB for RDS"
  type        = number
  default     = 20
}

variable "db_max_allocated_storage" {
  description = "Maximum storage autoscaling limit in GB (0 disables autoscaling)"
  type        = number
  default     = 50
}

variable "db_name" {
  description = "Database name"
  type        = string
  default     = "pulsegrid"
}

variable "db_username" {
  description = "Master username for RDS"
  type        = string
  default     = "pulsegrid"
  sensitive   = true
}

variable "db_password" {
  description = "Master password for RDS"
  type        = string
  sensitive   = true
}

variable "db_multi_az" {
  description = "Enable Multi-AZ for RDS"
  type        = bool
  default     = false
}

variable "enable_rds_performance_insights" {
  description = "Enable RDS Performance Insights (adds cost)"
  type        = bool
  default     = false
}

# --- S3 ---

variable "enable_s3_versioning" {
  description = "Enable S3 bucket versioning on source/output buckets"
  type        = bool
  default     = false
}

# --- Tags ---

variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default     = {}
}

variable "cost_center" {
  description = "Cost center billing tag"
  type        = string
  default     = "billing"
}

