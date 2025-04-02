variable "project" {
    description = "Project"
    default = "Big-Brother-Brasil"
}

variable "region" {
    description = "Region"
    default = "europe-west2"
}

variable "location" {
    description = "Project Location"
    default = "europe-west2"
}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
  
}

variable "gcs_bucket_name" {
    description = "My Storage Bucket Name"
    default = "bbb-bucket"
}

variable "credentials" {
    description = "My credentials"
    default = "./keys/my-creds.json" # This needs to be updated to the file location of you credentials json file.
}
