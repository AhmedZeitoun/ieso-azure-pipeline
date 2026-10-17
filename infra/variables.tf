variable "project_name" {
  type        = string
  description = "Project identifier used in resource naming"
  default     = "ieso-integration"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, uat, prod)"
  default     = "dev"
}

variable "location" {
  type        = string
  description = "Azure region for resource deployment"
  default     = "canadacentral"
}

variable "ieso_feed_url" {
  type        = string
  description = "Ontario IESO Public Hourly Generator Output by Fuel Type XML feed"
  default     = "https://reports-public.ieso.ca/public/GenOutputbyFuelHourly/PUB_GenOutputbyFuelHourly.xml"
}

variable "tags" {
  type        = map(string)
  description = "Resource tags applied to all Azure components"
  default = {
    Project     = "IESO Power Generation Integration"
    Owner       = "Data & Integration Developer"
    Sector      = "Nuclear Engineering - CANDU"
    Environment = "dev"
    ManagedBy   = "Terraform"
  }
}
