import os

import git

try:
    repo = git.Repo.clone_from("https://github.com/stikkireddy/export-repo", "../../tmp", branch='master')
except Exception as e:
    repo = git.Repo("../../tmp")

origin = repo.remote()
origin.pull()

def gitDiff(branch1, branch2):
    format = '--name-only'
    commits = []
    g = git.Git("../../tmp")
    differ = g.diff('%s..%s' % (branch1, branch2), format).split("\n")
    for line in differ:
        if len(line):
            commits.append(line)

    #for commit in commits:
    #    print '*%s' % (commit)
    return commits
for line in gitDiff("5b885da571366a9c1db542a66f6ad1738daf992f", "8865baf644f7f9ddaeb50f53dd17ec00d0a33592"):
    print(line)
# path = "/Users/Sri.Tikkireddy/PycharmProjects/databricks-terraformer/tmp/cluster_policies"
# policies = [os.path.join(path, item) for item in os.listdir("/Users/Sri.Tikkireddy/PycharmProjects/databricks-terraformer/tmp/cluster_policies")]
# #
# repo.git.add(policies)
# repo.index.commit("testmsg")
# origin = repo.remote(name='origin')
# origin._push()