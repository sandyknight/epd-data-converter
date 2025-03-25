use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use ::zip::ZipArchive;
use polars::prelude::*;
use std::fs;
use std::time::{Instant, Duration};

/// Extracts the single file from `zip_path` into the `destination_folder`,
/// preserving the original file name from inside the zip.
/// Returns the path to the extracted file.
///
/// # Arguments
/// * `zip_path` - The path to the ZIP file to extract.
/// * `destination_folder` - The folder where the file should be placed.
pub fn extract_data<P: AsRef<Path>, Q: AsRef<Path>>(
    zip_path: P,
    destination_folder: Q
) -> io::Result<PathBuf> {
    println!("Extracting: {}", zip_path.as_ref().display());

    // 1. Open the ZIP file
    let zip_file = match File::open(&zip_path) {
        Ok(file) => file,
        Err(e) => {
            println!("Error opening zip file: {}", e);
            return Err(e);
        }
    };

    // 2. Create a ZipArchive object
    let mut archive = match ZipArchive::new(zip_file) {
        Ok(archive) => archive,
        Err(e) => {
            let error = io::Error::new(io::ErrorKind::Other, format!("Failed to read zip archive: {}", e));
            println!("{}", error);
            return Err(error);
        }
    };

    // Check if the archive is empty
    if archive.len() == 0 {
        let error = io::Error::new(io::ErrorKind::Other, "Zip archive is empty");
        println!("{}", error);
        return Err(error);
    }

    // 3. Access the first file inside
    let mut file_in_zip = match archive.by_index(0) {
        Ok(file) => file,
        Err(e) => {
            let error = io::Error::new(io::ErrorKind::Other, format!("Failed to access file in zip: {}", e));
            println!("{}", error);
            return Err(error);
        }
    };

    // 4. Extract the original name from the ZIP entry
    let file_name_in_zip = file_in_zip.name().to_string();

    // 5. Construct your desired output path
    let out_path = destination_folder.as_ref().join(&file_name_in_zip);

    // Ensure the parent folder(s) exist
    if let Some(parent_dir) = out_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent_dir) {
            println!("Error creating directory {}: {}", parent_dir.display(), e);
            return Err(e);
        }
    }

    // 6. Create & write the extracted file
    let mut outfile = match File::create(&out_path) {
        Ok(file) => file,
        Err(e) => {
            println!("Error creating output file {}: {}", out_path.display(), e);
            return Err(e);
        }
    };

    // Copy the contents
    match io::copy(&mut file_in_zip, &mut outfile) {
        Ok(bytes) => println!("Extracted {} bytes to {}", bytes, out_path.display()),
        Err(e) => {
            println!("Error writing to output file: {}", e);
            return Err(e);
        }
    }

    Ok(out_path)
}

/// Converts a CSV file to Parquet format using the EPD schema.
///
/// # Arguments
/// * `csv_path` - Path to the CSV file to convert
/// * `parquet_path` - Destination path for the Parquet file
pub fn convert_data<P: AsRef<Path>, Q: AsRef<Path>>(csv_path: P, parquet_path: Q) -> Result<(), PolarsError> {
    let mut epd_schema = Schema::with_capacity(26);

    // Add fields to the schema - note the .into() for strings and correct DataType variants
    epd_schema.with_column("YEAR_MONTH".into(), DataType::Int64);
    epd_schema.with_column("REGIONAL_OFFICE_NAME".into(), DataType::String);
    epd_schema.with_column("REGIONAL_OFFICE_CODE".into(), DataType::String);
    epd_schema.with_column("ICB_NAME".into(), DataType::String);
    epd_schema.with_column("ICB_CODE".into(), DataType::String);
    epd_schema.with_column("PCO_NAME".into(), DataType::String);
    epd_schema.with_column("PCO_CODE".into(), DataType::String);
    epd_schema.with_column("PRACTICE_NAME".into(), DataType::String);
    epd_schema.with_column("PRACTICE_CODE".into(), DataType::String);
    epd_schema.with_column("ADDRESS_1".into(), DataType::String);
    epd_schema.with_column("ADDRESS_2".into(), DataType::String);
    epd_schema.with_column("ADDRESS_3".into(), DataType::String);
    epd_schema.with_column("ADDRESS_4".into(), DataType::String);
    epd_schema.with_column("POSTCODE".into(), DataType::String);
    epd_schema.with_column("BNF_CHEMICAL_SUBSTANCE".into(), DataType::String);
    epd_schema.with_column("CHEMICAL_SUBSTANCE_BNF_DESCR".into(), DataType::String);
    epd_schema.with_column("BNF_CODE".into(), DataType::String);
    epd_schema.with_column("BNF_DESCRIPTION".into(), DataType::String);
    epd_schema.with_column("BNF_CHAPTER_PLUS_CODE".into(), DataType::String);
    epd_schema.with_column("QUANTITY".into(), DataType::Float64);
    epd_schema.with_column("ITEMS".into(), DataType::Int64);
    epd_schema.with_column("TOTAL_QUANTITY".into(), DataType::Float64);
    epd_schema.with_column("ADQUSAGE".into(), DataType::Float64);
    epd_schema.with_column("NIC".into(), DataType::Float64);
    epd_schema.with_column("ACTUAL_COST".into(), DataType::Float64);
    epd_schema.with_column("UNIDENTIFIED".into(), DataType::String);

    // Try to read the CSV file
    let q = match LazyCsvReader::new(&csv_path)
        .with_has_header(true)
        .with_schema(Some(Arc::new(epd_schema)))
        .finish() {
            Ok(q) => q,
            Err(e) => {
                println!("Error reading CSV file: {}", e);
                return Err(e);
            }
        };

    // Try to collect the dataframe
    let mut df = match q.collect() {
        Ok(df) => df,
        Err(e) => {
            println!("Error collecting dataframe: {}", e);
            return Err(e);
        }
    };

    // Print a sample of the data
    println!("Sample data from {}", csv_path.as_ref().display());
    println!("{}", df.head(Some(5)));

    // Create the parquet file
    let mut file = match std::fs::File::create(&parquet_path) {
        Ok(file) => file,
        Err(e) => {
            let error = PolarsError::ComputeError(format!("Failed to create parquet file: {}", e).into());
            println!("{}", error);
            return Err(error);
        }
    };

    // Write the parquet file
    match ParquetWriter::new(&mut file).finish(&mut df) {
        Ok(_) => {
            println!("Successfully wrote parquet file: {}", parquet_path.as_ref().display());
            Ok(())
        },
        Err(e) => {
            println!("Error writing parquet file: {}", e);
            Err(e)
        }
    }
}

/// Processes a single zip file - extracts it, converts to parquet, and cleans up
fn process_zip_file(zip_path: &Path, data_dir: &Path) -> io::Result<()> {
    println!("\n===== Processing: {} =====", zip_path.display());
    let start_time = Instant::now();

    // Extract the file name without extension to use for the parquet file
    let file_stem = zip_path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");

    // Extract the zip file to CSV
    let csv_file_path = match extract_data(zip_path, data_dir) {
        Ok(path) => path,
        Err(e) => {
            println!("ERROR extracting {}: {}", zip_path.display(), e);
            return Err(e);
        }
    };

    // Generate the parquet filename
    let parquet_filename = format!("{}.parquet", file_stem);
    let parquet_path = data_dir.join(parquet_filename);

    // Convert CSV to Parquet
    match convert_data(&csv_file_path, &parquet_path) {
        Ok(_) => {
            println!("Successfully converted to: {}", parquet_path.display());

            // Delete the CSV file to save space
            println!("Removing CSV file: {}", csv_file_path.display());
            if let Err(e) = fs::remove_file(&csv_file_path) {
                println!("Warning: Failed to remove CSV file: {}", e);
            }

            // Delete the zip file to save space
            println!("Removing ZIP file: {}", zip_path.display());
            if let Err(e) = fs::remove_file(zip_path) {
                println!("Warning: Failed to remove ZIP file: {}", e);
            }

            let elapsed = start_time.elapsed();
            println!("Completed processing {} in {:.2?}", zip_path.display(), elapsed);
            Ok(())
        },
        Err(e) => {
            println!("ERROR converting file {}: {}", csv_file_path.display(), e);

            // Still try to remove the CSV file even if conversion failed
            // as it takes up space and won't be useful
            if let Err(csv_rm_err) = fs::remove_file(&csv_file_path) {
                println!("Warning: Failed to remove CSV after error: {}", csv_rm_err);
            }

            // Return a generic IO error since we have a PolarsError
            Err(io::Error::new(io::ErrorKind::Other, format!("Conversion error: {}", e)))
        }
    }
}

fn main() -> io::Result<()> {
    // Directory containing the zip files
    let data_dir = "/home/sjwk/projects/epd-presrcibing-data/data/";
    let data_dir_path = Path::new(data_dir);

    println!("Starting to process all ZIP files in {}", data_dir);
    let start_time = Instant::now();

    // Count files for progress reporting
    let mut total_files = 0;
    let mut processed_files = 0;
    let mut successful_files = 0;
    let mut failed_files = 0;

    // First count total zip files
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() && path.extension().map_or(false, |ext| ext == "zip") {
            total_files += 1;
        }
    }

    println!("Found {} ZIP files to process", total_files);

    // Process each zip file
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let path = entry.path();

        // Check if it's a zip file
        if path.is_file() && path.extension().map_or(false, |ext| ext == "zip") {
            processed_files += 1;
            println!("\nProgress: Processing file {}/{} ({}%)",
                     processed_files, total_files,
                     (processed_files as f64 / total_files as f64 * 100.0) as u32);

            match process_zip_file(&path, data_dir_path) {
                Ok(_) => {
                    successful_files += 1;
                },
                Err(_) => {
                    failed_files += 1;
                    // Continue with next file even if this one failed
                }
            }
        }
    }

    let total_time = start_time.elapsed();
    println!("\n===== Processing Summary =====");
    println!("Total files processed: {}/{}", processed_files, total_files);
    println!("Successful conversions: {}", successful_files);
    println!("Failed conversions: {}", failed_files);
    println!("Total processing time: {:.2?}", total_time);
    println!("Average time per file: {:.2?}",
             if processed_files > 0 {
                 total_time / processed_files as u32
             } else {
                 Duration::from_secs(0)
             });
    println!("All processing complete!");

    Ok(())
}
