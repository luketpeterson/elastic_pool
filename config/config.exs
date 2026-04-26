import Config

config :prolog_bridge,
  # Default to the quick-load file, then the standard pl file, then fallback to kb.pl
  kb_file: System.get_env("KB_FILE") || 
           (File.exists?("kb_large.qlf") && "kb_large.qlf") || 
           "kb.pl"
