ALTER TABLE [dbo].[Item] ADD PRIMARY KEY CLUSTERED 
(
	[ItemId] ASC,
    [VersionNbr] DESC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF) ON [PRIMARY]
GO

ALTER TABLE [dbo].[Customer]ADD PRIMARY KEY CLUSTERED 
(
	[CustomerId] DESC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [IX_Item_IdCustomer] ON [dbo].[Item]
(
	CustomerId ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF) ON [PRIMARY]
GO

CREATE NONCLUSTERED INDEX [IX_Item_DeletedFlag] ON [dbo].[Item]
(
	DeletedFlag ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF) ON [PRIMARY]
GO