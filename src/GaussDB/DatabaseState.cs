namespace HuaweiCloud.GaussDB;

enum DatabaseState : byte
{
    Unknown = 0,
    Offline = 1,
    PrimaryReadWrite = 2,
    PrimaryReadOnly = 3,
    Standby = 4
}
