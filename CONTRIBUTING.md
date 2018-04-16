Welcome to the LAGraph package.

We try to follow standard Github procedures.

If you find a bug please create an issue and then follow it up with a
pull request if you have a solution. Here are some notes on how to do
that ...

## LAGraph on GitHub

To contribute you'll need a GitHub account. If you don't already have
a GitHub account you'll need to [create
one](https://github.com/join?source=header-home).

If you discover an issue with with LAGraph then [open an
issue](https://github.com/ibm/lagraph/issues) and, optionally, assign
yourself.

If you find a issue that you are interested in working on then just
add a comment to the issue asking to be assigned the issue.

Once you have a GitHub account, go to the [LAGraph GitHub
site](https://github.com/ibm/lagraph) and click the Fork button to
fork a personal remote copy of the LAGraph repository to your GitHub
account.

The next step is to clone your LAGraph fork to your local machine.

	$ git clone https://github.com/YOUR_GITHUB_NAME/lagraph.git

Following this, it's a good idea to set your git user name and email
address. In addition, you may want to set the `push.default` property
to `simple`. You only need to execute these commands once.

	$ git config --global user.name "Your Name"
	$ git config --global user.email "yourname@yourhost.com"
	$ git config --global push.default simple

Next, reference the main LAGraph repository as a remote repository. By
convention, you can call this `upstream`. You only need to add the
remote `upstream` repository once.

	$ git remote add upstream https://github.com/ibm/lagraph.git

After this, you should have an `origin` repository, which references
your personal forked LAGraph repository on GitHub, and the `upstream`
repository, which references the main LAGraph repository on GitHub.

	$ git remote -v
	origin   https://github.com/YOUR_GITHUB_NAME/lagraph.git (fetch)
	origin   https://github.com/YOUR_GITHUB_NAME/lagraph.git (push)
	upstream https://github.com/ibm/lagraph.git (fetch)
	upstream https://github.com/ibm/lagraph.git (push)

The main code branch by convention is the `master` branch. You can check out the `master` branch using the `checkout` command:

	git checkout master

To update this branch with the latest official code, you can `pull`
from the `upstream` `master` branch. A `pull` essentially does a
`fetch` (retrieves code) and a `merge` (merges latest remote changes
into your local branch):

	git pull upstream master

It's recommended that you create a new, separate branch for your work
based on the current `master` branch. Give this branch a descriptive
name. For example, if you were assigned the issue `#101`, you
could use the `checkout -b` command to create a new branch based on
the `master` branch and check out this branch:

	git checkout -b ISSUE-101-my_cool_new_feature

At this point, you are ready to do your work on this branch.

If your updates involve code, you should run the complete test suite
to verify that your updates have not had unexpected side-effects in
the project:

	sbt/sbt test

Your commit messages should follow standard git formatting
conventions. If your commit is in regards to a particular issue,
please include a reference, such as in the
following:

	git commit -m "[# ISSUE-101] My cool new feature"

When ready, push your changes on this branch to your remote GitHub fork:

	$ git push
	fatal: The current branch ISSUE-101-my_cool_new_feature has no upstream branch.
	To push the current branch and set the remote as upstream, use
	
	    git push --set-upstream origin ISSUE-101-my_cool_new_feature
	
	$ git push --set-upstream origin ISSUE-101-my_cool_new_feature




At this stage, you can go to the GitHub web page for your fork and
file a Pull Request for the work that you did on this branch. A Pull
Request is a request for project committers (who have write access to
LAGraph) to review your code and integrate your code into the project.
Typically, you will see a green button to allow you to file a Pull
Request.

Once your Pull Request is opened at
[LAGraph Pull Requests](https://github.ibm/com/ibm/lagraph/pulls),
typically Jenkins will automatically build the project to see if all
tests pass when run for your particular branch. These automatic builds
can be seen
[here](https://sparktc.ibmcloud.com/jenkins/job/LAGraph-PullRequestBuilder/).

A conversation typically will proceed with regards to your Pull
Request. Project committers and potentially others will give you
useful feedback and potentially request that some changes be made to
your code. In response, you can make the requested updates or explain
why you feel that they make sense as they are. If you make additional
updates, you can commit the changes and then push the changes to your
remote branch. These updates will automatically appear in the pull
request.

When your changes are accepted (a committer will write "Looks good to
me", "LGTM", or something similar), a committer will attempt to
incorporate your changes into the LAGraph project. Typically this is
done by squashing all of your commits into a single commit and then
rebasing your changes into the master branch. Rebasing gives a linear
commit history to the project.

If the merge in complicated, it is possible that a committer may ask
you to resolve any merge conflicts in your pull request. If any
difficulties are experienced, a project committer will be more than
happy to assist in the integration of your work into the project.

After the Pull Request is closed, a comment can be added to the
original JIRA issue referencing the Pull Request, and the issue can be
resolved and closed.


Some core issues are documented in the [issues
section](https://ibm.github.io/lagraph/#issues) of the LAGraph
Documentation.

Any questions? Please feel free to contact @hornwp.

To view the result that github will render, under your project _Settings_, in the _GitHub Pages_ section for _Source_ select "master branch /docs folder"
