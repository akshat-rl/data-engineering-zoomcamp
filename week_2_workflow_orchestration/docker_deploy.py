from prefect.deployments import Deployment
from docker_test import main_flow
from prefect.infrastructure.docker import DockerContainer

docker_container_block = DockerContainer.load("zoomde")

docker_dep = Deployment.build_from_flow(
    flow=main_flow,
    name= 'docker_flow',
    infrastructure = docker_container_block

)


if __name__ == '__main__':
    docker_dep.apply()
    
   