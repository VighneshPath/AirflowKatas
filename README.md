# Apache Airflow Coding Kata

A comprehensive hands-on learning experience that guides developers through Apache Airflow concepts from basic to advanced levels. This kata includes practical exercises, relevant documentation links, and progressive challenges that build upon each other to ensure thorough understanding of workflow orchestration.

## 🎯 Learning Objectives

By completing this kata, you will:

- Master Apache Airflow fundamentals including DAGs, tasks, and operators
- Understand workflow orchestration concepts and best practices
- Learn advanced features like sensors, XComs, branching, and error handling
- Build real-world data pipelines and ETL processes
- Develop production-ready workflows with proper monitoring and alerting

## 📋 Prerequisites

- **Python 3.8+**: Basic Python programming knowledge
- **Docker & Docker Compose**: For running local Airflow environment
- **Git**: For cloning and managing the kata repository
- **Text Editor/IDE**: VS Code, PyCharm, or your preferred editor

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd airflow-coding-kata
```

### 2. Start Airflow Environment

```bash
# Start Airflow with Docker Compose
docker-compose up -d

# Wait for services to be ready (about 2-3 minutes)
docker-compose logs -f webserver
```

### 3. Access Airflow UI

- **URL**: http://localhost:8080
- **Username**: `airflow`
- **Password**: `airflow`

### 4. Verify Setup

```bash
# Check that all services are running
docker-compose ps

# Run the setup verification script
python scripts/verify_setup.py
```

## 📚 Learning Path

The kata is organized into 10 progressive modules:

| Module                                    | Topic                       | Duration    | Key Concepts                                   |
| ----------------------------------------- | --------------------------- | ----------- | ---------------------------------------------- |
| [01](modules/01-setup/)                   | Setup & Introduction        | 45-60 min   | Environment setup, basic concepts              |
| [02](modules/02-dag-fundamentals/)        | DAG Fundamentals            | 60-75 min   | DAG structure, scheduling, parameters          |
| [03](modules/03-tasks-operators/)         | Tasks & Operators           | 75-90 min   | BashOperator, PythonOperator, custom operators |
| [04](modules/04-scheduling-dependencies/) | Scheduling & Dependencies   | 60-75 min   | Complex scheduling, dependency patterns        |
| [05](modules/05-sensors-triggers/)        | Sensors & Triggers          | 75-90 min   | FileSensor, S3KeySensor, custom sensors        |
| [06](modules/06-data-passing-xcoms/)      | Data Passing & XComs        | 60-75 min   | Inter-task communication, data flow            |
| [07](modules/07-branching-conditionals/)  | Branching & Conditionals    | 75-90 min   | Dynamic workflows, conditional logic           |
| [08](modules/08-error-handling/)          | Error Handling & Monitoring | 60-75 min   | Retries, callbacks, SLAs, alerting             |
| [09](modules/09-advanced-patterns/)       | Advanced Patterns           | 90-120 min  | TaskGroups, dynamic DAGs, optimization         |
| [10](modules/10-real-world-projects/)     | Real-World Projects         | 120-180 min | ETL pipelines, API integrations                |

**Total Estimated Time**: 12-16 hours

## 🏗️ Project Structure

```
airflow-coding-kata/
├── README.md                    # This file
├── docker-compose.yml           # Local Airflow environment
├── requirements.txt             # Python dependencies
├── modules/                     # Learning modules (01-10)
│   ├── 01-setup/
│   ├── 02-dag-fundamentals/
│   └── ...
├── dags/                        # Example and exercise DAGs
├── plugins/                     # Custom operators and hooks
├── data/                        # Sample data files
├── solutions/                   # Solution implementations
├── resources/                   # Additional learning materials
└── scripts/                     # Utility scripts
```

## 🎓 How to Use This Kata

### For Individual Learning

1. **Start with Module 1**: Follow the modules in order
2. **Read Concepts**: Understand theory before exercises
3. **Complete Exercises**: Hands-on practice is essential
4. **Check Solutions**: Compare your work with provided solutions
5. **Explore Resources**: Dive deeper with additional links

### For Team Training

1. **Setup Workshop Environment**: Use Docker for consistency
2. **Pair Programming**: Work through exercises together
3. **Code Reviews**: Review solutions as a team
4. **Discussion Sessions**: Share learnings and challenges
5. **Extended Projects**: Build on the real-world scenarios

### For Instructors

1. **Preparation**: Complete the kata yourself first
2. **Environment Setup**: Ensure all participants can run Docker
3. **Pacing**: Allow extra time for questions and debugging
4. **Customization**: Adapt exercises to your specific use cases
5. **Assessment**: Use the provided validation scripts

## 🛠️ Troubleshooting

### Common Issues

**Docker Issues**

```bash
# Reset Docker environment
docker-compose down -v
docker-compose up -d
```

**Port Conflicts**

```bash
# Check what's using port 8080
lsof -i :8080

# Or modify docker-compose.yml to use different ports
```

**Permission Issues**

```bash
# Fix file permissions
sudo chown -R $USER:$USER .
```

### Getting Help

- **Documentation**: Check module-specific troubleshooting guides
- **Issues**: Create GitHub issues for bugs or unclear instructions
- **Community**: Join the Airflow Slack community
- **Official Docs**: [Apache Airflow Documentation](https://airflow.apache.org/docs/)

## 🤝 Contributing

We welcome contributions to improve the kata! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Ways to Contribute

- **Bug Reports**: Found an issue? Let us know!
- **Exercise Improvements**: Better examples or clearer instructions
- **New Modules**: Advanced topics or specialized use cases
- **Documentation**: Typos, clarifications, or additional resources
- **Translations**: Help make the kata accessible in other languages

## 📖 Additional Resources

### Official Documentation

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Airflow Concepts](https://airflow.apache.org/docs/apache-airflow/stable/concepts/index.html)

### Community Resources

- [Airflow Slack Community](https://apache-airflow-slack.herokuapp.com/)
- [Airflow GitHub Repository](https://github.com/apache/airflow)
- [Awesome Airflow](https://github.com/jghoman/awesome-apache-airflow)

### Advanced Learning

- [Airflow Summit Talks](https://airflowsummit.org/)
- [Data Engineering Podcast - Airflow Episodes](https://www.dataengineeringpodcast.com/)
- [Astronomer Academy](https://academy.astronomer.io/)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Airflow community for the amazing orchestration platform
- Contributors who helped improve this learning resource
- Organizations using Airflow who shared their experiences and best practices

---

**Ready to start your Airflow journey?** Begin with [Module 1: Setup & Introduction](modules/01-setup/)!
