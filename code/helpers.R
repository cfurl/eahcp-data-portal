# --- User inputs (reusable) ---------------------------------------------------
local_file     <- "C:/Users/cfurl/OneDrive - Edwards Aquifer Authority/r/eahcp-data-portal/data/fd/dropnet/cm_darters_with_zeros_one_row_one_drop-SUBSET.csv"
s3_destination <- "s3://eahcp-data-portal/fd-data/"   # prefix (folder) in S3

# Path to .Renviron in the project root (assumes you run this from the repo root)
renviron_path  <- file.path(".Renviron")

# --- Load AWS creds from .Renviron -------------------------------------------
if (!file.exists(renviron_path)) {
  stop("Can't find .Renviron at: ", renviron_path, "\nRun this script from the project root or set renviron_path explicitly.")
}

readRenviron(renviron_path)

# --- Packages ----------------------------------------------------------------
if (!requireNamespace("arrow", quietly = TRUE)) install.packages("arrow")
if (!requireNamespace("readr", quietly = TRUE)) install.packages("readr")

# --- Build output names -------------------------------------------------------
base_name     <- tools::file_path_sans_ext(basename(local_file))
local_parquet <- file.path(tempdir(), paste0(base_name, ".parquet"))

# Ensure destination ends with a slash
if (!grepl("/$", s3_destination)) s3_destination <- paste0(s3_destination, "/")
s3_object_uri <- paste0(s3_destination, basename(local_parquet))

# --- Read CSV -> write Parquet ------------------------------------------------
df <- readr::read_csv(local_file, show_col_types = FALSE)
arrow::write_parquet(df, local_parquet, compression = "snappy")
message("Wrote parquet: ", local_parquet)

# --- Upload to S3 (AWS CLI uses env vars loaded above) ------------------------
aws_args <- c("s3", "cp", local_parquet, s3_object_uri, "--only-show-errors")

res <- system2("aws", aws_args, stdout = TRUE, stderr = TRUE)
cat(paste(res, collapse = "\n"), "\n")

message("Uploaded to: ", s3_object_uri)