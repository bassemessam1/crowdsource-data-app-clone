resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-actions-pool"
  display_name              = "GitHub Actions Pool"
  description               = "Workload Identity Pool for GitHub Actions CI/CD"
  project                   = var.project_id
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub OIDC Provider"
  project                            = var.project_id

  # GitHub's OIDC issuer — GCP fetches public keys from here
  # to verify the signature on every token GitHub generates
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  # Attribute mapping — translates GitHub JWT claims into
  # GCP attributes used in IAM conditions
  attribute_mapping = {
    "google.subject"              = "assertion.sub"
    "attribute.actor"             = "assertion.actor"
    "attribute.repository"        = "assertion.repository"
    "attribute.repository_owner"  = "assertion.repository_owner"
  }

  # Attribute condition — security gate.
  # Only tokens from YOUR GitHub user/org are trusted.
  attribute_condition = "assertion.repository_owner == '${var.github_owner}'"
}

# Grant GitHub Actions permission to impersonate sa-terraform
resource "google_service_account_iam_member" "github_terraform_wi" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/sa-terraform@${var.project_id}.iam.gserviceaccount.com"
  role               = "roles/iam.workloadIdentityUser"

  # principalSet matches any GitHub token with this repository attribute
  member = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_owner}/opensignal-gcp-clone"
}
