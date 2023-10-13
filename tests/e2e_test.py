import json

import pulumi
import pytest
from pulumi import FileAsset, Config, ResourceOptions
from pulumi.automation import create_or_select_stack, ConfigValue, UpResult
from pulumi_aws import s3, mwaa, ec2, iam, get_caller_identity, get_availability_zones

from tests.conftest import manual_tests


@pytest.fixture
def astro_deployment(
    name,
    version,
):
    # TODO
    # sh_args = {"_in": sys.stdin, "_out": sys.stdout, "_err": sys.stderr, "_tee": True}
    sh = pytest.importorskip("sh")
    sh.astro("deployment", "create")


@pytest.fixture
def mwaa_instance(request) -> UpResult:
    (
        stack_name,
        project_name,
        region,
        name,
        version,
        _cidr_block,
        start_cidr_offset,
    ) = request.param
    stack = create_or_select_stack(
        stack_name=stack_name,
        project_name=project_name,
        program=_create_mwaa,
    )
    stack.workspace.install_plugin("aws", "v5.0.0")
    stack.set_all_config(
        {
            "aws:region": ConfigValue(region),
            "aws:defaultTags": ConfigValue(
                json.dumps(
                    {
                        "tags": {
                            "managed_by": "pulumi",
                            "pulumi_stack": stack_name,
                            "pulumi_project": project_name,
                        }
                    }
                )
            ),
            "version": ConfigValue(version),
            "name": ConfigValue(name),
            "start_cidr_offset": ConfigValue(start_cidr_offset),
            "_cidr_block": ConfigValue(_cidr_block),
        }
    )
    up_result = stack.up(on_output=print)  # also stack.refresh
    print(
        f"Update Summary: \n{json.dumps(up_result.summary.resource_changes, indent=4)}"
    )
    yield up_result
    destroy_result = stack.destroy()
    print(
        f"Destroy Summary: \n{json.dumps(destroy_result.summary.resource_changes, indent=4)}"
    )


def _create_mwaa():
    def cidr_block(plus: int = 0) -> str:
        return _cidr_block.format(0 + plus)

    requirements_txt = "requirements.txt"
    super_rad_dag_py = "super_rad_dag.py"
    aws_config = Config("aws")
    other_config = Config()

    name = other_config.require("name")
    print(f"name: {name}")

    version = other_config.require("version")
    print(f"version: {version}")

    _cidr_block = other_config.require("_cidr_block")
    print(f"_cidr_block: {_cidr_block}")

    start_cidr_offset = int(other_config.require("start_cidr_offset"))
    print(f"start_cidr_offset: {start_cidr_offset}")

    region = aws_config.require("region")
    acct = get_caller_identity().account_id
    azs = get_availability_zones(state="available")

    # S3 BUCKET
    # https://docs.aws.amazon.com/mwaa/latest/userguide/mwaa-s3-bucket.html
    bucket = s3.BucketV2(
        name,
        bucket=name,
    )
    s3.BucketAclV2(name, bucket=bucket.id, acl="private")
    s3.BucketPublicAccessBlock(
        name,
        bucket=bucket.id,
        block_public_acls=True,
        block_public_policy=True,
        ignore_public_acls=True,
        restrict_public_buckets=True,
    )
    s3.BucketVersioningV2(
        name,
        bucket=bucket.id,
        versioning_configuration=s3.BucketVersioningV2VersioningConfigurationArgs(
            status="Enabled"
        ),
    )

    # REQUIREMENTS.TXT
    requirements = s3.BucketObjectv2(
        requirements_txt,
        key=requirements_txt,
        bucket=bucket.id,
        source=FileAsset(f"resources/{requirements_txt}"),
    )

    # DAGS
    s3.BucketObjectv2(
        super_rad_dag_py,
        key=f"dags/{super_rad_dag_py}",
        bucket=bucket.id,
        source=FileAsset(f"resources/dags/{super_rad_dag_py}"),
    )

    # NETWORKING
    # https://docs.aws.amazon.com/mwaa/latest/userguide/networking-about.html
    vpc = ec2.Vpc(
        name,
        cidr_block=f"{cidr_block(plus=0)}/16",
        enable_dns_support=True,
        enable_dns_hostnames=True,
    )
    route_table = ec2.RouteTable(f"{name}-route-table", vpc_id=vpc.id)
    ec2.MainRouteTableAssociation(
        f"{name}-route-table-association", route_table_id=route_table.id, vpc_id=vpc.id
    )

    internet_gateway = ec2.InternetGateway(name)
    internet_gateway_attachment = ec2.InternetGatewayAttachment(
        name, internet_gateway_id=internet_gateway.id, vpc_id=vpc.id
    )

    ec2.Route(
        f"{name}-default-public-route",
        route_table_id=route_table.id,
        destination_cidr_block="0.0.0.0/0",
        gateway_id=internet_gateway.id,
        opts=ResourceOptions(depends_on=[internet_gateway_attachment]),
    )
    sg = ec2.SecurityGroup(
        name,
        vpc_id=vpc.id,
        ingress=[
            ec2.SecurityGroupIngressArgs(
                protocol="-1", from_port=0, to_port=0, self=True
            )
        ],
        egress=[
            ec2.SecurityGroupEgressArgs(
                protocol="-1", from_port=0, to_port=0, cidr_blocks=["0.0.0.0/0"]
            )
        ],
    )

    subnet_ids = []
    for _i in range(2):
        i = _i + 1

        # PUBLIC
        subnet_resource = ec2.Subnet(
            f"{name}-subnet-{i}",
            vpc_id=vpc.id,
            availability_zone=azs.names[_i],
            cidr_block=f"{cidr_block(start_cidr_offset + i)}/24",
            map_public_ip_on_launch=True,
        )
        nat_gateway_eip = ec2.Eip(
            f"{name}-nat-gateway-eip-{i}",
            vpc=True,
            opts=ResourceOptions(depends_on=[internet_gateway_attachment]),
        )
        nat_gateway = ec2.NatGateway(
            f"{name}-nat-gateway-{i}",
            allocation_id=nat_gateway_eip.allocation_id,
            subnet_id=subnet_resource.id,
        )
        ec2.RouteTableAssociation(
            f"{name}-subnet-route-table-association-{i}",
            route_table_id=route_table.id,
            subnet_id=subnet_resource.id,
        )

        # PRIVATE
        private_subnet_resource = ec2.Subnet(
            f"{name}-private-subnet-{i}",
            vpc_id=vpc.id,
            availability_zone=azs.names[_i],
            cidr_block=f"{cidr_block(start_cidr_offset + 10 + i)}/24",
            map_public_ip_on_launch=False,
        )
        subnet_ids.append(private_subnet_resource.id)
        private_route_table = ec2.RouteTable(
            f"{name}-private-route-table-{i}", vpc_id=vpc.id
        )
        ec2.Route(
            f"{name}-default-private-route-{i}",
            route_table_id=private_route_table.id,
            destination_cidr_block="0.0.0.0/0",
            nat_gateway_id=nat_gateway.id,
        )
        ec2.RouteTableAssociation(
            f"{name}-private-subnet-route-table-association-{i}",
            route_table_id=private_route_table.id,
            subnet_id=private_subnet_resource.id,
        )

    # IAM
    # https://docs.aws.amazon.com/mwaa/latest/userguide/mwaa-create-role.html
    role = iam.Role(
        name,
        inline_policies=[
            iam.RoleInlinePolicyArgs(
                name=name,
                policy=bucket.bucket.apply(
                    lambda _bucket: json.dumps(
                        {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Action": "airflow:PublishMetrics",
                                    "Resource": f"arn:aws:airflow:{region}:*:environment/{name}",
                                },
                                {
                                    "Effect": "Deny",
                                    "Action": "s3:ListAllMyBuckets",
                                    "Resource": [
                                        f"arn:aws:s3:::{_bucket}",
                                        f"arn:aws:s3:::{_bucket}/*",
                                    ],
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": [
                                        "s3:GetObject*",
                                        "s3:GetBucket*",
                                        "s3:List*",
                                    ],
                                    "Resource": [
                                        f"arn:aws:s3:::{_bucket}",
                                        f"arn:aws:s3:::{_bucket}/*",
                                    ],
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": [
                                        "logs:CreateLogStream",
                                        "logs:CreateLogGroup",
                                        "logs:PutLogEvents",
                                        "logs:GetLogEvents",
                                        "logs:GetLogRecord",
                                        "logs:GetLogGroupFields",
                                        "logs:GetQueryResults",
                                    ],
                                    "Resource": [
                                        f"arn:aws:logs:{region}:*:log-group:airflow-{name}-*"
                                    ],
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": ["logs:DescribeLogGroups"],
                                    "Resource": ["*"],
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": ["s3:GetAccountPublicAccessBlock"],
                                    "Resource": ["*"],
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": "cloudwatch:PutMetricData",
                                    "Resource": "*",
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": [
                                        "sqs:ChangeMessageVisibility",
                                        "sqs:DeleteMessage",
                                        "sqs:GetQueueAttributes",
                                        "sqs:GetQueueUrl",
                                        "sqs:ReceiveMessage",
                                        "sqs:SendMessage",
                                    ],
                                    "Resource": f"arn:aws:sqs:{region}:*:airflow-celery-*",
                                },
                                {
                                    "Effect": "Allow",
                                    "Action": [
                                        "kms:Decrypt",
                                        "kms:DescribeKey",
                                        "kms:GenerateDataKey*",
                                        "kms:Encrypt",
                                    ],
                                    "NotResource": f"arn:aws:kms:*:{acct}:key/*",
                                    "Condition": {
                                        "StringLike": {
                                            "kms:ViaService": [
                                                "sqs.us-east-1.amazonaws.com"
                                            ]
                                        }
                                    },
                                },
                            ],
                        }
                    )
                ),
            )
        ],
        assume_role_policy=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": [
                                "airflow-env.amazonaws.com",
                                "airflow.amazonaws.com",
                            ]
                        },
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
    )

    airflow = mwaa.Environment(
        name,
        name=name,
        airflow_version=version,
        dag_s3_path="dags/",
        source_bucket_arn=bucket.arn,
        environment_class="mw1.small",
        min_workers=1,
        max_workers=1,
        requirements_s3_path=requirements_txt,
        requirements_s3_object_version=requirements.version_id,
        # plugins_s3_object_version="",
        # plugins_s3_path
        airflow_configuration_options={"core.parallelism": "99"},
        schedulers=2,  # needs to be 2 for >=2.2.2
        execution_role_arn=role.arn,
        network_configuration=mwaa.EnvironmentNetworkConfigurationArgs(
            security_group_ids=[sg.id], subnet_ids=subnet_ids
        ),
        webserver_access_mode="PUBLIC_ONLY",
    )
    pulumi.export("airflow_version", airflow.airflow_version)
    pulumi.export("dag_s3_path", airflow.dag_s3_path)
    pulumi.export("webserver_url", airflow.webserver_url)
    pulumi.export(
        "airflow_configuration_options", airflow.airflow_configuration_options
    )
    pulumi.export("environment_class", airflow.environment_class)
    pulumi.export("requirements_s3_path", airflow.requirements_s3_path)
    pulumi.export("plugins_s3_path", airflow.plugins_s3_path)
    pulumi.export("status", airflow.status)


@pytest.mark.skip
@manual_tests  # requires a real user, who ran `astro login` recently",
@pytest.mark.slow_integration_test
@pytest.mark.parametrize(
    "mwaa_instance",
    [
        # stack_name: str, project_name: str, region: str, name: str,
        # version: str, _cidr_block: str, start_cidr_offset: int
        [
            "e2e",
            "starship-mwaa-2-2-2",
            "us-east-1",
            "starship-mwaa-2-2-2",
            "2.2.2",
            "10.192.{}.0",
            "10",
        ]
    ],
    indirect=True,  # this passes the above through to MWAA
)
def test_e2e(mwaa_instance: UpResult):
    mwaa_instance.outputs.get()
    # install starship
    # create astro deployment
