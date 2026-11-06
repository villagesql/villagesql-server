# Skips the test because of a known issue filed in suite/knn_review/ISSUES.md,
# unless the environment sets KNN_REVIEW_ALL (to check whether it is fixed).
#
# Usage, first thing in a test:
#   --let $knn_issue = KNN-3 A KNN scan that stops early never ends its cursor
#   --source suite/knn_review/include/known_issue.inc

if (!$KNN_REVIEW_ALL) {
  --skip $knn_issue
}
