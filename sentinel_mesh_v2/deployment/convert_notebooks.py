import json
import os
from pathlib import Path


def py_to_ipynb_cells(py_content: str) -> list:
    """Converts a python file separated by # %% cell markers into Jupyter/Fabric cells."""
    cells = []
    current_source = []
    lines = py_content.split("\n")
    
    for line in lines:
        if line.strip().startswith("# %%"):
            if current_source:
                # Add previous cell
                cells.append({
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": [l + "\n" for l in current_source]
                })
                current_source = []
        else:
            current_source.append(line)
            
    if current_source:
        cells.append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [l + "\n" for l in current_source]
        })
        
    return cells


def create_fabric_notebook(name: str, py_file_path: Path, output_dir: Path):
    """Creates a Fabric-native .Notebook directory structure."""
    notebook_dir = output_dir / f"{name}.Notebook"
    os.makedirs(notebook_dir, exist_ok=True)
    
    # Read python file
    with open(py_file_path, "r", encoding="utf-8") as f:
        py_content = f.read()
        
    # Generate cells
    cells = py_to_ipynb_cells(py_content)
    
    # Create notebook-content.ipynb
    notebook_content = {
        "cells": cells,
        "metadata": {
            "language_info": {
                "name": "python"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 2
    }
    
    with open(notebook_dir / "notebook-content.ipynb", "w", encoding="utf-8") as f:
        json.dump(notebook_content, f, indent=2)
        
    # Create item.metadata.json
    metadata = {
        "type": "Notebook",
        "displayName": name
    }
    with open(notebook_dir / "item.metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
        
    print(f"[CONVERT] Created Fabric Notebook: {name}.Notebook")


def main():
    project_root = Path(__file__).parent.parent.parent
    notebooks_src_dir = project_root / "sentinel_mesh_v2" / "notebooks"
    
    # We write the Fabric items directly into the repository root because
    # Fabric Git integration reads from the root directory.
    create_fabric_notebook(
        "L4_graph_analysis", 
        notebooks_src_dir / "L4_graph_analysis.py", 
        project_root
    )
    
    create_fabric_notebook(
        "L9_recalibration", 
        notebooks_src_dir / "L9_recalibration.py", 
        project_root
    )


if __name__ == "__main__":
    main()
