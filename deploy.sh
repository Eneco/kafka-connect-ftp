IMAGE=eneco/kafka-connect-ftp
TAG=$(git describe --exact-match)

if [ "$TAG" ]; then
  docker login -u="$DOCKER_USER" -p="$DOCKER_PASS" &&
  docker tag $IMAGE:$TAG $IMAGE:latest &&
  docker push $IMAGE:latest && \
  docker push $IMAGE:$TAG && \
  echo published tagged commit $IMAGE:$TAG
else
  echo "This commit does not have an annotated tag, thus an artifact won't be published."
fi
