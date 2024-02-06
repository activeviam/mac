# Contributing to MAC

## Creating issues

* **Check if the issue was not already reported** in the
 [issues](https://github.com/activeviam/mac/issues) of the repository
* Assign the appropriate labels to the issue: `bug`, `enhancement`, `help wanted`, `question`...

## Creating and merging pull requests

Pull Requests are only open for ActiveViam contributors to create.

At least one `activeviam/activepivot-core-owner` reviewer must approve the Pull Request before it
can be merged. It is recommended to review the Pull Request beforehand with another ActiveViam
contributor.

In order to merge a Pull Request inside the `main` branch of the repository, the following steps must be verified:
 - Compiling code in Java 11
 - Passing test battery
 - Passing legacy compatibility check
 - Passing Sonar quality gate
 - Passing checkstyle check

#### Formatting and Checkstyle
 
Contributions are expected to follow the guidelines of the ActivePivot repository regarding formatting and checkstyle.

### Versioning

The MAC repository has a single `main` branch which is compatible with all ActivePivot versions from 5.8 and onward.

A legacy branch of the mac repository named `5.6` is compatible with the data exported by ActivePivot's 5.6.X and 5.7.X.
This branch is deprecated and no longer supported and is kept for legacy purposes .

