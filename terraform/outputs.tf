output "eks_cluster_name" {
  value = aws_eks_cluster.pulsegrid.name
}

output "eks_cluster_endpoint" {
  value = aws_eks_cluster.pulsegrid.endpoint
}

output "eks_oidc_provider_arn" {
  value = aws_iam_openid_connect_provider.eks.arn
}

output "s3_source_bucket" {
  value = aws_s3_bucket.source.id
}

output "s3_output_bucket" {
  value = aws_s3_bucket.output.id
}

output "rds_endpoint" {
  value = aws_db_instance.pulsegrid.endpoint
}

output "rds_database_name" {
  value = aws_db_instance.pulsegrid.db_name
}

output "api_iam_role_arn" {
  description = "Paste into kube/rbac.yaml's pulsegrid-api ServiceAccount eks.amazonaws.com/role-arn annotation"
  value       = aws_iam_role.api.arn
}

output "worker_iam_role_arn" {
  description = "Paste into kube/rbac.yaml's pulsegrid-worker ServiceAccount eks.amazonaws.com/role-arn annotation"
  value       = aws_iam_role.worker.arn
}

output "kubeconfig_command" {
  value = "aws eks update-kubeconfig --region ${var.aws_region} --name ${aws_eks_cluster.pulsegrid.name}"
}
