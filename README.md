# Spark Name Finder

This project implements a distributed application using Apache Spark to find specific strings (names) in a large text file. The application reads a large text file, searches for occurrences of a list of names, and outputs the locations of these names in the text. The results are aggregated and printed after all text parts have been processed.

## Features

- **Distributed Processing**: Leverages Spark's distributed computing capabilities to process large text files efficiently.
- **Pattern Matching**: Uses regular expressions to find occurrences of a list of names in the text.
- **Parallel Execution**: Each line of the text is processed in parallel to speed up the search.
- **Broadcast Variables**: Optimizes performance by broadcasting the list of names to all executor nodes.

## Getting Started

### Prerequisites

- Java 8 or higher
- Apache Maven
- Apache Spark 3.0 or higher

### Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/spark-name-finder.git
   cd spark-name-finder/name-finder
   mvn clean install
