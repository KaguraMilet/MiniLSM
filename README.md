Primarily inspired by [MiniLSM](https://skyzh.github.io/mini-lsm)

The main purpose of the project is to serve as a prototype to help me implement some of my ideas about LSM.

## Why not RocksDB?
RocksDB is too complex and too large. And, I hate C++!

## Ideas implemented and to be implemented
- [x] Hash Sharing: [Reducing Bloom Filter CPU Overhead in LSM-Trees on Modern Storage Devices](https://dl.acm.org/doi/10.1145/3465998.3466002)
- [ ] Hybrid Latch: [Scalable and robust latches for database systems](https://dl.acm.org/doi/10.1145/3399666.3399908)
- [ ] SHaMba: [LSM-Tree Under (Memory) Pressure](https://cs-people.bu.edu/mathan/publications/adms22-mun.pdf)
- [ ] [Range Cache: An Efficient Cache Component for Accelerating Range Queries on LSM Based Key-Value Stores](https://www.computer.org/csdl/proceedings-article/icde/2024/171500a488/1YOtAxDn2IU)

...

## License

The Mini-LSM starter code and solution are under [Apache 2.0 license](LICENSE). The author reserves the full copyright of the tutorial materials (markdown files and figures).
