# Pulsegrid - Development (minimal cost)
# Usage: terraform plan -var-file=environments/dev.tfvars -var="db_password=<secret>"

environment = "dev"
region      = "us-east-1"

# 2 AZs, single NAT gateway
az_count           = 2
enable_nat_gateway = true

# EKS - smallest practical sizing
eks_cluster_version           = "1.29"
enable_eks_control_plane_logs = false
api_node_instance_types       = ["t3.small"]
api_node_min_size             = 1
api_node_max_size             = 2
api_node_desired_size         = 1
worker_node_instance_types    = ["t3.large"]
worker_node_capacity_type     = "SPOT"
worker_node_min_size          = 0
worker_node_max_size          = 3
worker_node_desired_size      = 1

# RDS - single-AZ micro instance
db_instance_class               = "db.t4g.micro"
db_allocated_storage            = 20
db_max_allocated_storage        = 50
db_multi_az                     = false
enable_rds_performance_insights = false

# S3
enable_s3_versioning = false
