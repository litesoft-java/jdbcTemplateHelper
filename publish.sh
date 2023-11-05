GPG_TTY=$(tty)
export GPG_TTY
plannedTag=v$(./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout)
if git show-ref --tags ${plannedTag} --quiet; then
  echo "${plannedTag} tag already exists, update your POM version"
  exit 1
fi
./mvnw deploy -Possrh
echo "Adding tag: ${plannedTag}"
git tag "${plannedTag}"
