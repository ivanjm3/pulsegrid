# Pulsegrid - Production (still lean; scale via tfvars)
# Usage: terraform plan -var-file=environments/prod.tfvars -var="db_password=<secret>"

environment = "prod"
region      = "us-east-1"

az_count           = 2
enable_nat_gateway = true

eks_cluster_version           = "1.29"
enable_eks_control_plane_logs = true
api_node_instance_types       = ["t3.medium"]
api_node_min_size             = 2
api_node_max_size             = 5
api_node_desired_size         = 2
worker_node_instance_types    = ["c5.xlarge"]
worker_node_capacity_type     = "ON_DEMAND"
worker_node_min_size          = 1
worker_node_max_size          = 20
worker_node_desired_size      = 2

db_instance_class               = "db.t3.small"
db_allocated_storage            = 50
db_max_allocated_storage        = 200
db_multi_az                     = true
enable_rds_performance_insights = true

enable_s3_versioning = true

# Tags
cost_center = "billing"

