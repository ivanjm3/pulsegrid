# -----------------------------------------------------------------------------
# Pulsegrid Infrastructure Outputs
# -----------------------------------------------------------------------------

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "vpc_cidr_block" {
  description = "VPC CIDR block"
  value       = aws_vpc.main.cidr_block
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = aws_subnet.private[*].id
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = aws_subnet.public[*].id
}

output "eks_cluster_name" {
  description = "EKS cluster name"
  value       = aws_eks_cluster.main.name
}

output "eks_cluster_endpoint" {
  description = "EKS cluster API endpoint"
  value       = aws_eks_cluster.main.endpoint
}

output "eks_cluster_certificate_authority" {
  description = "EKS cluster CA certificate (base64)"
  value       = aws_eks_cluster.main.certificate_authority[0].data
  sensitive   = true
}

output "eks_cluster_oidc_issuer" {
  description = "EKS OIDC issuer URL (for IRSA)"
  value       = aws_eks_cluster.main.identity[0].oidc[0].issuer
}

output "eks_node_role_arn" {
  description = "IAM role ARN for EKS nodes"
  value       = aws_iam_role.eks_nodes.arn
}

output "s3_source_bucket_name" {
  description = "S3 bucket for source video uploads"
  value       = aws_s3_bucket.source.id
}

output "s3_source_bucket_arn" {
  description = "S3 source bucket ARN"
  value       = aws_s3_bucket.source.arn
}

output "s3_output_bucket_name" {
  description = "S3 bucket for transcoded outputs"
  value       = aws_s3_bucket.output.id
}

output "s3_output_bucket_arn" {
  description = "S3 output bucket ARN"
  value       = aws_s3_bucket.output.arn
}

output "rds_endpoint" {
  description = "RDS Postgres endpoint (host:port)"
  value       = aws_db_instance.main.endpoint
}

output "rds_hostname" {
  description = "RDS Postgres hostname"
  value       = aws_db_instance.main.address
}

output "rds_port" {
  description = "RDS Postgres port"
  value       = aws_db_instance.main.port
}

output "rds_database_name" {
  description = "RDS database name"
  value       = aws_db_instance.main.db_name
}

output "sg_eks_cluster_id" {
  description = "EKS cluster security group ID"
  value       = aws_security_group.eks_cluster.id
}

output "sg_eks_nodes_id" {
  description = "EKS nodes security group ID"
  value       = aws_security_group.eks_nodes.id
}

output "sg_rds_id" {
  description = "RDS security group ID"
  value       = aws_security_group.rds.id
}

output "database_url" {
  description = "Postgres connection string (without password)"
  value       = "postgres://${var.db_username}@${aws_db_instance.main.address}:${aws_db_instance.main.port}/${var.db_name}?sslmode=require"
  sensitive   = true
}
