# Contributing Guide

Thank you for your interest in **by-framework-java**! We welcome contributions from the community.

## 🛠️ Development Environment

Before you start contributing, please ensure your development environment meet the following requirements:

- **Java**: JDK 21 or higher
- **Maven**: 3.8 or higher
- **Redis**: 7.0 or higher (for local testing)
- **IDE**: IntelliJ IDEA or VS Code is recommended

## 🚀 Contribution Flow

1. **Fork the repository** to your own GitHub account.
2. **Clone the repository** locally:
   ```bash
   git clone https://github.com/your-username/by-framework-java.git
   ```
3. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```
4. **Development**:
   - Ensure your code follows the Checkstyle rules (run `mvn validate` to check).
   - Please write unit tests for new features or bug fixes.
5. **Run Tests**:
   ```bash
   mvn clean verify
   ```
6. **Submit Changes**:
   ```bash
   git commit -m "feat: describe your changes"
   ```
   *We recommend using [Conventional Commits](https://www.conventionalcommits.org/).*
7. **Push to Remote**:
   ```bash
   git push origin feature/your-feature-name
   ```
8. **Raise a Pull Request**: Go to the original repository and create a PR. Please follow the PR template.

## 📏 Code Style

We use Checkstyle to enforce consistent code style. The configuration is located at `config/checkstyle/checkstyle.xml`.
Please run the following command before submitting a PR:
```bash
mvn checkstyle:check
```

## 🐛 Reporting Issues

If you find a bug or have a feature suggestion, please submit it via [GitHub Issues](https://github.com/beyonai/by-framework-java/issues).
Please provide detailed descriptions and reproduction steps.

## 📄 License

By contributing to this project, you agree that your contributions will be licensed under the [Apache 2.0 License](LICENSE).
