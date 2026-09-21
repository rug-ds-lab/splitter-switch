# S4: Stateful and Scalable Splitter Switch (2024 IFIP Networking Conference) 

This repository contains the source code and experimental scripts for S4 Splitter Switch; A network-centric data parallelization framework supporting window-based parallel operator execution for Complex Event Processing (CEP) using programmable packet processor.

---

## Hardware & Software Requirements

* **Target:** Intel Tofino 1 ASIC (or Tofino Model simulator)
* **SDK:** Intel Barefoot SDE v9.9.0
* **Traffic Generation:** TRex / P4 Packet Generator
* **Testing Framework:** Python 3.8+, PTF, Scapy

---

## Reproduction Workflow

# 1. Build P4 Pipeline
./scripts/build.sh

# 2. Environment Setup
./scripts/setup.sh

# 3. Run Test Suite
./scripts/run_tofino_model.sh   # Terminal 1
./scripts/run_app.sh            # Terminal 2
./scripts/run_ptf_tests.sh      # Terminal 3


## Citation

If you build upon this work or use the splitter-switch repository, please cite our IFIP Networking paper:

```bibtex
@inproceedings{boughzala2024innetwork,
  author    = {Boughzala, Bochra and Koldehofe, Boris},
  title     = {In-Network Management of Parallel Data Streams over Programmable Data Planes},
  booktitle = {2024 IFIP Networking Conference (IFIP Networking)},
  pages     = {50--58},
  year      = {2024},
  doi       = {10.23919/IFIPNetworking62109.2024.10619822}
}
```