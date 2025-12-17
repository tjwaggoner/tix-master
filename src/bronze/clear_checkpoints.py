# Databricks notebook source
"""
Clear Bronze Layer Checkpoints

This notebook clears Auto Loader checkpoints to force a fresh schema inference.
Run this when you need to reset the schema evolution state.
"""

# COMMAND ----------

# Configuration
CATALOG = "ticket_master"
BRONZE_SCHEMA = "bronze"
VOLUME_NAME = "raw_data"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}/_checkpoints"

# COMMAND ----------

print("=" * 60)
print("CLEARING BRONZE LAYER CHECKPOINTS")
print("=" * 60)
print(f"Checkpoint base: {CHECKPOINT_BASE}\n")

# List entity types
entity_types = ['events', 'venues', 'attractions', 'classifications']

for entity_type in entity_types:
    checkpoint_path = f"{CHECKPOINT_BASE}/{entity_type}"
    
    try:
        # Check if checkpoint exists
        files = dbutils.fs.ls(checkpoint_path)
        
        if files:
            print(f"📁 {entity_type}:")
            print(f"   Found {len(files)} checkpoint files/dirs")
            
            # Remove checkpoint
            dbutils.fs.rm(checkpoint_path, recurse=True)
            print(f"   ✓ Cleared checkpoint: {checkpoint_path}\n")
        else:
            print(f"⚠️  {entity_type}: No checkpoint files found\n")
            
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            print(f"⚠️  {entity_type}: No checkpoint directory\n")
        else:
            print(f"✗ {entity_type}: Error - {str(e)}\n")

print("=" * 60)
print("✓ Checkpoint cleanup complete!")
print("=" * 60)
print("\n💡 You can now run bronze_auto_loader to start with a fresh schema.")

