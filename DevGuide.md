

# Introduction #

First, let's give you some background of the project.

## Licensing ##
All Supersonic Query Engine source and pre-built packages are provided under the [Apache License 2.0](http://opensource.org/licenses/Apache-2.0).

## The Supersonic Community ##
The Supersonic community exists primarily through the [discussion group](http://groups.google.com/group/supersonic-query-engine), the [issue tracker](https://code.google.com/p/supersonic/issues/list) and, to a lesser extent, the [source control repository](https://code.google.com/p/supersonic/source/checkout). You are definitely encouraged to contribute to the discussion and you can also help us to keep the effectiveness of the group high by following and promoting the guidelines listed here.

### Please Be Friendly ###
Showing courtesy and respect to others is a vital part of the Google culture, and we strongly encourage everyone participating in Supersonic development to join us in accepting nothing less. Of course, being courteous is not the same as failing to constructively disagree with each other, but it does mean that we should be respectful of each other when enumerating the 42 technical reasons that a particular proposal may not be the best choice. There's never a reason to be antagonistic or dismissive toward anyone who is sincerely trying to contribute to a discussion.

Sure, the Supersonic project is intended to be a serious business and all that, but it also should be a lot of fun. Let's keep it that way. Let's strive to be one of the friendliest communities in all of open source.

### Where to Discuss Supersonic ###
Please discuss Supersonic in the official [Supersonic Query Engine Group](http://groups.google.com/group/supersonic-query-engine). You don't have to actually submit code in order to sign up. Your participation itself is a valuable contribution.

# Working with the Code #
If you want to get your hands dirty with the code inside Supersonic, this is the section for you.

## Checking Out the Source from Git ##
Checking out the Supersonic source is most useful if you plan to tweak it yourself. You check out the source for Supersonic using a Git client as you would for any other project hosted on Google Code. Please see the instruction on the [source code access page](https://code.google.com/p/supersonic/source/checkout) for how to do it.

## Compiling from Source ##
Once you check out the code, you can find instructions on how to compile it in the [INSTALL](https://code.google.com/p/supersonic/source/browse/INSTALL) file.

## Testing ##
Testing is crucial to keep any project at a decent level of quality. Tests should be written for any new code, and changes should be verified to not break existing tests before they are submitted for review. To perform the tests, run `make check` after successful compilation.

# Contributing Code #
We are very happy that Supersonic is now open source, and hope to get great patches from the community. Before you start hammering away at a new feature, though, please take the time to read this section and understand the process. While it seems rigorous, we want to keep a high standard of quality in the code base.

## Contributor License Agreements ##
You must sign a Contributor License Agreement (CLA) before we can accept any code. The CLA protects you and us.

  * If you are an individual writing original source code and you're sure you own the intellectual property, then you'll need to sign an [individual CLA](http://code.google.com/legal/individual-cla-v1.0.html).
  * If you work for a company that wants to allow you to contribute your work to Supersonic, then you'll need to sign a [corporate CLA](http://code.google.com/legal/corporate-cla-v1.0.html).
Follow either of the two links above to access the appropriate CLA and instructions for how to sign and return it.

## Coding Style ##
To keep the source consistent, readable, diffable and easy to merge, we use a fairly rigid coding style, as defined by the [google-styleguide](http://code.google.com/p/google-styleguide/) project. All patches will be expected to conform to the style outlined here.

## Submitting Patches ##
Please do submit code. Here's what you need to do:

  1. Decide which code you want to submit. A submission should be a set of changes that addresses one issue in the Supersonic issue tracker. Please don't mix more than one logical change per submittal, because it makes the history hard to follow. If you want to make a change that doesn't have a corresponding issue in the issue tracker, please create one.
  1. Also, coordinate with team members that are listed on the issue in question. This ensures that work isn't being duplicated and communicating your plan early also generally leads to better patches.
  1. Ensure that your code adheres to the Supersonic source code style.
  1. Ensure that there are unit tests for your code.
  1. Sign a Contributor License Agreement.
  1. Create a patch file using `git format-patch` with appropriate options. Use `-M` to handle renames. Here are some resources to help you out: a neatly formatted [man page](http://git-scm.com/docs/git-format-patch), a [chapter](http://git-scm.com/book/en/Distributed-Git-Contributing-to-a-Project) on patch submission from the timeless [Git Book](http://git-scm.com/book) and a link to some [guidelines](https://github.com/git/git/blob/master/Documentation/SubmittingPatches) used by Git developers themselves.
  1. Send the generated patch to `supersonic-query-engine@googlegroups.com` .

---

This page is based on the [Making GWT Better](http://code.google.com/webtoolkit/makinggwtbetter.html) guide from the [Google Web Toolkit](http://code.google.com/webtoolkit/) project. Except as otherwise noted, the content of this page is licensed under the [Creative Commons Attribution 2.5 License](http://creativecommons.org/licenses/by/2.5/).