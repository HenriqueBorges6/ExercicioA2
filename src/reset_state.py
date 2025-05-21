import os
import shutil

def reset_markers():
    marker_dir = os.path.join("src","markers")
    if os.path.exists(marker_dir):
        for f in os.listdir(marker_dir):
            path = os.path.join(marker_dir, f)
            if os.path.isfile(path):
                os.remove(path)
        print(f"Marcadores resetados ({marker_dir}/).")
    else:
        print("Pasta 'markers/' não existe.")

def reset_csvs():
    csv_dir = "transformed_data"
    if os.path.exists(csv_dir):
        for f in os.listdir(csv_dir):
            path = os.path.join(csv_dir, f)
            if os.path.isfile(path) and f.endswith(".csv"):
                os.remove(path)
        print(f"CSVs resetados ({csv_dir}/).")
    else:
        print("Pasta 'transformed_data/' não existe.")
def reset_csvs_metrics():
    csv_dir = os.path.join("src","transformed_data")
    if os.path.exists(csv_dir):
        for f in os.listdir(csv_dir):
            path = os.path.join(csv_dir, f)
            if os.path.isfile(path) and f.endswith(".csv"):
                os.remove(path)
        print(f"CSVs resetados ({csv_dir}/).")
    else:
        print("Pasta 'transformed_data/' não existe.")

if __name__ == "__main__":
    reset_markers()
    reset_csvs()
