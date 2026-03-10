# this will download a parquet, save it as a csv, read csv, and print head


# --- User inputs (reusable) ---------------------------------------------------
s3_parquet_uri <- "s3://eahcp-data-portal/fd-data/cm_darters_with_zeros_one_row_one_drop-SUBSET.parquet"
out_dir        <- getwd()   # where the CSV will be written

# Path to .Renviron (optional, but recommended if you store AWS creds there)
renviron_path  <- ".Renviron"
if (file.exists(renviron_path)) readRenviron(renviron_path)

# --- Packages ----------------------------------------------------------------
if (!requireNamespace("arrow", quietly = TRUE)) install.packages("arrow")
if (!requireNamespace("readr", quietly = TRUE)) install.packages("readr")
if (!requireNamespace("dplyr", quietly = TRUE)) install.packages("dplyr")

# --- Helpers -----------------------------------------------------------------
ensure_trailing_slash <- function(x) if (grepl("/$", x)) x else paste0(x, "/")

# --- Validate output dir ------------------------------------------------------
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

# --- Read Parquet from S3 -----------------------------------------------------
# Arrow can read directly from s3:// if your env has AWS creds
# (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / optional AWS_SESSION_TOKEN)
tbl <- arrow::open_dataset(s3_parquet_uri, format = "parquet")

# Collect into memory (fine for a small file; for huge files, filter/select first)
df <- dplyr::collect(tbl)

# --- Write CSV locally --------------------------------------------------------
base_name <- tools::file_path_sans_ext(basename(s3_parquet_uri))
csv_path  <- file.path(out_dir, paste0(base_name, ".csv"))

readr::write_csv(df, csv_path)
message("Wrote CSV: ", csv_path)

# --- Read CSV back in + print head() -----------------------------------------
df2 <- readr::read_csv(csv_path, show_col_types = FALSE)
print(utils::head(df2))