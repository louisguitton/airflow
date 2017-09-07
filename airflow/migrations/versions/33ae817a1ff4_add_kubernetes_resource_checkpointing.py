#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""kubernetes_resource_checkpointing

Revision ID: 33ae817a1ff4
Revises: 947454bf1dff
Create Date: 2017-09-11 15:26:47.598494

"""

# revision identifiers, used by Alembic.
revision = '33ae817a1ff4'
down_revision = 'd2ae31099d61'
branch_labels = None
depends_on = None


from alembic import op
import sqlalchemy as sa


RESOURCE_TABLE = "kube_resource_version"


def upgrade():
    table = op.create_table(
        RESOURCE_TABLE,
        sa.Column("one_row_id", sa.Boolean, server_default=sa.true(), primary_key=True),
        sa.Column("resource_version", sa.String(255)),
        sa.CheckConstraint("one_row_id", name="kube_resource_version_one_row_id")
    )
    op.bulk_insert(table, [
        {"resource_version": ""}
    ])


def downgrade():
    op.drop_table(RESOURCE_TABLE)
