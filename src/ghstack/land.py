#!/usr/bin/env python3

import logging
import re
from typing import List, Tuple

import ghstack.git
import ghstack.github
import ghstack.github_utils
import ghstack.shell
from ghstack.diff import PullRequestResolved
from ghstack.types import GitCommitHash


def lookup_pr_to_orig_ref_and_closed(
    github: ghstack.github.GitHubEndpoint, *, owner: str, name: str, number: int
) -> Tuple[str, bool]:
    logging.info(f"Looking up PR #{number} in {owner}/{name}")
    pr_result = github.graphql(
        """
        query ($owner: String!, $name: String!, $number: Int!) {
            repository(name: $name, owner: $owner) {
                pullRequest(number: $number) {
                    headRefName
                    closed
                }
            }
        }
    """,
        owner=owner,
        name=name,
        number=number,
    )
    pr = pr_result["data"]["repository"]["pullRequest"]
    head_ref = pr["headRefName"]
    closed = pr["closed"]
    assert isinstance(head_ref, str)
    orig_ref = re.sub(r"/head$", "/orig", head_ref)
    if orig_ref == head_ref:
        raise RuntimeError(
            "The ref {} doesn't look like a ghstack reference".format(head_ref)
        )
    logging.info(f"Found PR #{number}: head_ref={head_ref}, orig_ref={orig_ref}, closed={closed}")
    return orig_ref, closed


def main(
    pull_request: str,
    remote_name: str,
    github: ghstack.github.GitHubEndpoint,
    sh: ghstack.shell.Shell,
    github_url: str,
    *,
    force: bool = False,
) -> None:

    logging.info(f"Starting land process for PR: {pull_request}")
    logging.info(f"Remote name: {remote_name}, GitHub URL: {github_url}, Force: {force}")

    # We land the entire stack pointed to by a URL.
    # Local state is ignored; PR is source of truth
    # Furthermore, the parent commits of PR are ignored: we always
    # take the canonical version of the patch from any given pr

    logging.info("Parsing pull request URL/number")
    params = ghstack.github_utils.parse_pull_request(
        pull_request, sh=sh, remote_name=remote_name
    )
    logging.info(f"Parsed PR params: {params}")

    logging.info("Getting repository info and default branch")
    default_branch = ghstack.github_utils.get_github_repo_info(
        github=github,
        sh=sh,
        repo_owner=params["owner"],
        repo_name=params["name"],
        github_url=github_url,
        remote_name=remote_name,
    )["default_branch"]
    logging.info(f"Default branch: {default_branch}")

    logging.info("Checking branch protection settings")
    needs_force = False
    try:
        protection = github.get(
            f"repos/{params['owner']}/{params['name']}/branches/{default_branch}/protection"
        )
        if not protection["allow_force_pushes"]["enabled"]:
            logging.error(f"Default branch {default_branch} is protected and doesn't allow force pushes")
            raise RuntimeError(
                f"""\
Default branch {default_branch} is protected, and doesn't allow force pushes.
ghstack land does not work.  You will not be able to land your ghstack; please
resubmit your PRs using the normal pull request flow.

See https://github.com/ezyang/ghstack/issues/50 for more details, or
to complain to the ghstack authors."""
            )
        else:
            needs_force = True
            logging.info("Branch protection allows force pushes, will use --force-with-lease")
    except ghstack.github.NotFoundError:
        logging.info("No branch protection found, regular push will be used")
        pass

    logging.info("Looking up original PR reference and status")
    orig_ref, closed = lookup_pr_to_orig_ref_and_closed(
        github,
        owner=params["owner"],
        name=params["name"],
        number=params["number"],
    )

    if closed:
        logging.error("PR is already closed, cannot land it!")
        raise RuntimeError("PR is already closed, cannot land it!")

    if sh is None:
        # Use CWD
        sh = ghstack.shell.Shell()

    # Get up-to-date
    logging.info(f"Fetching latest changes from {remote_name}")
    sh.git("fetch", "--prune", remote_name)
    remote_orig_ref = remote_name + "/" + orig_ref
    logging.info(f"Remote original ref: {remote_orig_ref}")

    base = GitCommitHash(
        sh.git("merge-base", f"{remote_name}/{default_branch}", remote_orig_ref)
    )
    logging.info(f"Merge base: {base}")

    # compute the stack of commits in chronological order (does not
    # include base)
    logging.info("Computing stack of commits to land")
    stack = ghstack.git.parse_header(
        sh.git("rev-list", "--reverse", "--header", "^" + base, remote_orig_ref),
        github_url=github_url,
    )
    logging.info(f"Found {len(stack)} commits in stack")

    # Switch working copy
    logging.info("Saving current branch reference")
    try:
        prev_ref = sh.git("symbolic-ref", "--short", "HEAD")
    except RuntimeError:
        prev_ref = sh.git("rev-parse", "HEAD")
    logging.info(f"Previous ref: {prev_ref}")

    # If this fails, we don't have to reset
    logging.info(f"Checking out {remote_name}/{default_branch}")
    sh.git("checkout", f"{remote_name}/{default_branch}")

    try:
        # Compute the metadata for each commit
        logging.info("Computing metadata for each commit in stack")
        stack_orig_refs: List[Tuple[str, PullRequestResolved]] = []
        for i, s in enumerate(stack):
            pr_resolved = s.pull_request_resolved
            # We got this from GitHub, this better not be corrupted
            assert pr_resolved is not None

            logging.info(f"Processing commit {i+1}/{len(stack)}: PR #{pr_resolved.number}")
            ref, closed = lookup_pr_to_orig_ref_and_closed(
                github,
                owner=pr_resolved.owner,
                name=pr_resolved.repo,
                number=pr_resolved.number,
            )
            if closed and not force:
                logging.info(f"Skipping closed PR #{pr_resolved.number} (use --force to include)")
                continue
            stack_orig_refs.append((ref, pr_resolved))

        logging.info(f"Will land {len(stack_orig_refs)} commits")

        # OK, actually do the land now
        logging.info("Starting cherry-pick process")
        for i, (orig_ref, pr_resolved) in enumerate(stack_orig_refs):
            logging.info(f"Cherry-picking commit {i+1}/{len(stack_orig_refs)}: {remote_name}/{orig_ref} (PR #{pr_resolved.number})")
            try:
                sh.git("cherry-pick", f"{remote_name}/{orig_ref}")
                logging.info(f"Successfully cherry-picked PR #{pr_resolved.number}")
            except BaseException:
                logging.error(f"Failed to cherry-pick PR #{pr_resolved.number}, aborting")
                sh.git("cherry-pick", "--abort")
                raise

        # All good! Push!
        logging.info("All commits cherry-picked successfully, pushing to remote")
        maybe_force_arg = []
        if needs_force:
            maybe_force_arg = ["--force-with-lease"]
            logging.info("Using --force-with-lease for push")

        push_cmd = ["push"] + maybe_force_arg + [remote_name, f"HEAD:refs/heads/{default_branch}"]
        logging.info(f"Pushing with command: git {' '.join(push_cmd)}")
        sh.git(*push_cmd)
        logging.info("Successfully pushed commits to default branch")

        # Advance base to head to "close" the PR for all PRs.
        # This happens after the cherry-pick and push, because the cherry-picks
        # can fail (merge conflict) and the push can also fail (race condition)

        # TODO: It might be helpful to advance orig to reflect the true
        # state of upstream at the time we are doing the land, and then
        # directly *merge* head into base, so that the PR accurately
        # reflects what we ACTUALLY merged to master, as opposed to
        # this synthetic thing I'm doing right now just to make it look
        # like the PR got closed

        logging.info("Advancing base references to close PRs")
        for orig_ref, pr_resolved in stack_orig_refs:
            # TODO: regex here so janky
            base_ref = re.sub(r"/orig$", "/base", orig_ref)
            head_ref = re.sub(r"/orig$", "/head", orig_ref)
            logging.info(f"Advancing base ref for PR #{pr_resolved.number}: {base_ref}")
            sh.git(
                "push", remote_name, f"{remote_name}/{head_ref}:refs/heads/{base_ref}"
            )
            logging.info(f"Notifying GitHub that PR #{pr_resolved.number} is merged")
            github.notify_merged(pr_resolved)

        # Delete the branches
        logging.info("Cleaning up remote branches")
        for orig_ref, pr_resolved in stack_orig_refs:
            # TODO: regex here so janky
            base_ref = re.sub(r"/orig$", "/base", orig_ref)
            head_ref = re.sub(r"/orig$", "/head", orig_ref)
            logging.info(f"Deleting branches for PR #{pr_resolved.number}: {orig_ref}, {base_ref}")
            try:
                sh.git("push", remote_name, "--delete", orig_ref, base_ref)
                logging.info(f"Successfully deleted orig and base branches for PR #{pr_resolved.number}")
            except RuntimeError:
                # Whatever, keep going
                logging.warning(f"Failed to delete orig/base branches for PR #{pr_resolved.number}, continuing", exc_info=True)
            # Try deleting head_ref separately since often after it's merged it doesn't exist anymore
            logging.info(f"Deleting head branch for PR #{pr_resolved.number}: {head_ref}")
            try:
                sh.git("push", remote_name, "--delete", head_ref)
                logging.info(f"Successfully deleted head branch for PR #{pr_resolved.number}")
            except RuntimeError:
                # Whatever, keep going
                logging.warning(f"Failed to delete head branch for PR #{pr_resolved.number}, continuing", exc_info=True)

    finally:
        logging.info(f"Restoring previous branch: {prev_ref}")
        sh.git("checkout", prev_ref)

    logging.info("Land process completed successfully!")
