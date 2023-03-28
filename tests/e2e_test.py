import json
from sys import argv

from pulumi import FileAsset, Config, ResourceOptions
from pulumi.automation import create_or_select_stack, LocalWorkspaceOptions, ConfigValue
from pulumi_aws import s3, mwaa, ec2, iam, get_caller_identity, get_availability_zones

VERSION = "2.2.2"
NAME = f"starship-mwaa-{VERSION.replace('.', '-')}"
REQUIREMENTS_TXT = "requirements.txt"
SUPER_RAD_DAG_PY = "super_rad_dag.py"
START_CIDR_OFFSET = 10
_CIDR_BLOCK = "10.192.{}.0"


def cidr_block(plus: int = 0) -> str:
    return _CIDR_BLOCK.format(0+plus)


def pulumi_program():
    aws_config = Config("aws")
    region = aws_config.require("region")
    acct = get_caller_identity().account_id
    azs = get_availability_zones(state="available")

    # S3 BUCKET
    # https://docs.aws.amazon.com/mwaa/latest/userguide/mwaa-s3-bucket.html
    bucket = s3.BucketV2(NAME, bucket=NAME, )
    s3.BucketAclV2(NAME, bucket=bucket.id, acl="private")
    s3.BucketPublicAccessBlock(
        NAME,
        bucket=bucket.id,
        block_public_acls=True,
        block_public_policy=True,
        ignore_public_acls=True,
        restrict_public_buckets=True
    )
    s3.BucketVersioningV2(
        NAME,
        bucket=bucket.id,
        versioning_configuration=s3.BucketVersioningV2VersioningConfigurationArgs(status="Enabled")
    )

    # REQUIREMENTS.TXT
    requirements = s3.BucketObjectv2(
        REQUIREMENTS_TXT,
        key=REQUIREMENTS_TXT,
        bucket=bucket.id,
        source=FileAsset(f"resources/{REQUIREMENTS_TXT}"),
    )

    # DAGS
    dag = s3.BucketObjectv2(
        SUPER_RAD_DAG_PY,
        key=f"dags/{SUPER_RAD_DAG_PY}",
        bucket=bucket.id,
        source=FileAsset(f"resources/dags/{SUPER_RAD_DAG_PY}"),
    )

    # NETWORKING
    # https://docs.aws.amazon.com/mwaa/latest/userguide/networking-about.html
    vpc = ec2.Vpc(
        NAME,
        cidr_block=f"{cidr_block(plus=0)}/16",
        enable_dns_support=True,
        enable_dns_hostnames=True,
    )
    route_table = ec2.RouteTable(f"{NAME}-route-table", vpc_id=vpc.id)
    ec2.MainRouteTableAssociation(f"{NAME}-route-table-association", route_table_id=route_table.id, vpc_id=vpc.id)

    internet_gateway = ec2.InternetGateway(NAME)
    internet_gateway_attachment = ec2.InternetGatewayAttachment(
        NAME,
        internet_gateway_id=internet_gateway.id,
        vpc_id=vpc.id
    )

    ec2.Route(
        f"{NAME}-default-public-route",
        route_table_id=route_table.id,
        destination_cidr_block="0.0.0.0/0",
        gateway_id=internet_gateway.id,
        opts=ResourceOptions(depends_on=[internet_gateway_attachment])
    )
    sg = ec2.SecurityGroup(
        NAME,
        vpc_id=vpc.id,
        ingress=[ec2.SecurityGroupIngressArgs(protocol='-1', from_port=0, to_port=0, self=True)],
        egress=[ec2.SecurityGroupEgressArgs(protocol='-1', from_port=0, to_port=0, cidr_blocks=["0.0.0.0/0"])]
    )

    subnet_ids = []
    for _i in range(2):
        i = _i + 1

        # PUBLIC
        subnet_resource = ec2.Subnet(
            f"{NAME}-subnet-{i}",
            vpc_id=vpc.id,
            availability_zone=azs.names[_i],
            cidr_block=f"{cidr_block(START_CIDR_OFFSET + i)}/24",
            map_public_ip_on_launch=True,
        )
        nat_gateway_eip = ec2.Eip(
            f"{NAME}-nat-gateway-eip-{i}", vpc=True,
            opts=ResourceOptions(depends_on=[internet_gateway_attachment])
        )
        nat_gateway = ec2.NatGateway(
            f"{NAME}-nat-gateway-{i}",
            allocation_id=nat_gateway_eip.allocation_id,
            subnet_id=subnet_resource.id
        )
        ec2.RouteTableAssociation(
            f"{NAME}-subnet-route-table-association-{i}",
            route_table_id=route_table.id,
            subnet_id=subnet_resource.id
        )

        # PRIVATE
        private_subnet_resource = ec2.Subnet(
            f"{NAME}-private-subnet-{i}",
            vpc_id=vpc.id,
            availability_zone=azs.names[_i],
            cidr_block=f"{cidr_block(START_CIDR_OFFSET + 10 + i)}/24",
            map_public_ip_on_launch=False,
        )
        subnet_ids.append(private_subnet_resource.id)
        private_route_table = ec2.RouteTable(
            f"{NAME}-private-route-table-{i}",
            vpc_id=vpc.id
        )
        ec2.Route(
            f"{NAME}-default-private-route-{i}",
            route_table_id=private_route_table.id,
            destination_cidr_block="0.0.0.0/0",
            nat_gateway_id=nat_gateway.id
        )
        ec2.RouteTableAssociation(
            f"{NAME}-private-subnet-route-table-association-{i}",
            route_table_id=private_route_table.id,
            subnet_id=private_subnet_resource.id
        )

    # IAM
    # https://docs.aws.amazon.com/mwaa/latest/userguide/mwaa-create-role.html
    role = iam.Role(
        NAME,
        inline_policies=[iam.RoleInlinePolicyArgs(
            name=NAME,
            policy=bucket.bucket.apply(lambda _bucket: json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "airflow:PublishMetrics",
                        "Resource": f"arn:aws:airflow:{region}:*:environment/{NAME}"
                    },
                    {
                        "Effect": "Deny",
                        "Action": "s3:ListAllMyBuckets",
                        "Resource": [
                            f"arn:aws:s3:::{_bucket}",
                            f"arn:aws:s3:::{_bucket}/*"
                        ]
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject*",
                            "s3:GetBucket*",
                            "s3:List*"
                        ],
                        "Resource": [
                            f"arn:aws:s3:::{_bucket}",
                            f"arn:aws:s3:::{_bucket}/*"
                        ]
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
                            "logs:GetQueryResults"
                        ],
                        "Resource": [
                            f"arn:aws:logs:{region}:*:log-group:airflow-{NAME}-*"
                        ]
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:DescribeLogGroups"
                        ],
                        "Resource": [
                            "*"
                        ]
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetAccountPublicAccessBlock"
                        ],
                        "Resource": [
                            "*"
                        ]
                    },
                    {
                        "Effect": "Allow",
                        "Action": "cloudwatch:PutMetricData",
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "sqs:ChangeMessageVisibility",
                            "sqs:DeleteMessage",
                            "sqs:GetQueueAttributes",
                            "sqs:GetQueueUrl",
                            "sqs:ReceiveMessage",
                            "sqs:SendMessage"
                        ],
                        "Resource": f"arn:aws:sqs:{region}:*:airflow-celery-*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:GenerateDataKey*",
                            "kms:Encrypt"
                        ],
                        "NotResource": f"arn:aws:kms:*:{acct}:key/*",
                        "Condition": {
                            "StringLike": {
                                "kms:ViaService": [
                                    "sqs.us-east-1.amazonaws.com"
                                ]
                            }
                        }
                    }
                ],
            }))
        )],
        assume_role_policy=json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": [
                            "airflow-env.amazonaws.com",
                            "airflow.amazonaws.com"
                        ]
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        })
    )

    airflow = mwaa.Environment(
        NAME,
        name=NAME,
        airflow_version=VERSION,
        dag_s3_path="dags/",
        source_bucket_arn=bucket.arn,
        environment_class="mw1.small",
        min_workers=1,
        max_workers=1,
        requirements_s3_path=REQUIREMENTS_TXT,
        requirements_s3_object_version=requirements.version_id,
        # plugins_s3_object_version="",
        # plugins_s3_path
        airflow_configuration_options={
            "core.parallelism": "99"
        },
        schedulers=2,  # needs to be 2 for >=2.2.2
        execution_role_arn=role.arn,
        network_configuration=mwaa.EnvironmentNetworkConfigurationArgs(
            security_group_ids=[sg.id],
            subnet_ids=subnet_ids
        ),
        webserver_access_mode="PUBLIC_ONLY"
    )


# ############################ #
# required
# curl -fsSL https://get.pulumi.com | sh
# export PATH="$HOME/.pulumi/bin:$PATH"

# PulumiAutoHook.get_conn #
# project_name = conn.extra_dejson.get("extra__pulumi__project_name")
# stack_name = conn.extra_dejson.get("extra__pulumi__stack_name")
# backend_url = conn.host

# if conn.password:
#     self.env_vars["PULUMI_ACCESS_TOKEN"] = conn.password

# config_passphrase = conn.extra_dejson.get("extra__pulumi__config_passphrase")
# if config_passphrase:
#     env_vars["PULUMI_CONFIG_PASSPHRASE"] = config_passphrase


stack = create_or_select_stack(
    stack_name="stack_name",
    project_name="project_name",
    program=pulumi_program,
    opts=LocalWorkspaceOptions(
        # env_vars=env_vars,
        # if backend_url:
        #     stack_opts.project_settings = auto.ProjectSettings(
        #         name="project_name",
        #         runtime="python",
        #         backend=auto.ProjectBackend(url="backend_url"),
        #     )
    ),
)
stack.workspace.install_plugin("aws", "v5.0.0")
stack.set_config("aws:region", ConfigValue("us-east-1"))
stack.set_config("aws:defaultTags", ConfigValue(json.dumps(
    {"tags": {"managed_by": "pulumi"}}
)))


if len(argv) > 1 and argv[1] == "destroy":
    result = stack.destroy(on_output=print)
    print(f"update summary: \n{json.dumps(result.summary.resource_changes, indent=4)}")
elif len(argv) > 1 and argv[1] == "refresh":
    result = stack.refresh(on_output=print)
    print(f"refresh summary: \n{json.dumps(result.summary.resource_changes, indent=4)}")
else:
    result = stack.up(on_output=print)
    print(f"update summary: \n{json.dumps(result.summary.resource_changes, indent=4)}")
