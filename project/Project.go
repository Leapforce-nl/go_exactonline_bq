package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	project "github.com/leapforce-libraries/go_exactonline_new/project"
	go_bigquery "github.com/leapforce-libraries/go_google/bigquery"
	types "github.com/leapforce-libraries/go_types"
)

type Project struct {
	OrganisationID_            int64
	SoftwareClientLicenceID_   int64
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	ID                         string
	Account                    string
	AccountCode                string
	AccountContact             string
	AccountName                string
	AllowAdditionalInvoicing   bool
	BlockEntry                 bool
	BlockRebilling             bool
	BudgetedAmount             float64
	BudgetedCosts              float64
	BudgetedRevenue            float64
	BudgetOverrunHours         byte
	BudgetType                 int64
	BudgetTypeDescription      string
	Classification             string
	ClassificationDescription  string
	Code                       string
	CostsAmountFC              float64
	Created                    bigquery.NullTimestamp
	Creator                    string
	CreatorFullName            string
	CustomerPONumber           string
	Description                string
	Division                   int64
	DivisionName               string
	EndDate                    bigquery.NullTimestamp
	FixedPriceItem             string
	FixedPriceItemDescription  string
	HasWBSLines                bool
	InternalNotes              string
	InvoiceAsQuoted            bool
	Manager                    string
	ManagerFullname            string
	MarkupPercentage           float64
	Modified                   bigquery.NullTimestamp
	Modifier                   string
	ModifierFullName           string
	Notes                      string
	PrepaidItem                string
	PrepaidItemDescription     string
	PrepaidType                int64
	PrepaidTypeDescription     string
	SalesTimeQuantity          float64
	SourceQuotation            string
	StartDate                  bigquery.NullTimestamp
	TimeQuantityToAlert        float64
	Type                       int64
	TypeDescription            string
	UseBillingMilestones       bool
}

func getProject(c *project.Project, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string) Project {
	t := time.Now()

	return Project{
		organisationID,
		softwareClientLicenceID,
		softwareClientLicenseGuid,
		t, t,
		c.ID.String(),
		c.Account.String(),
		c.AccountCode,
		c.AccountContact.String(),
		c.AccountName,
		c.AllowAdditionalInvoicing,
		c.BlockEntry,
		c.BlockRebilling,
		c.BudgetedAmount,
		c.BudgetedCosts,
		c.BudgetedRevenue,
		c.BudgetOverrunHours,
		c.BudgetType,
		c.BudgetTypeDescription,
		c.Classification.String(),
		c.ClassificationDescription,
		c.Code,
		c.CostsAmountFC,
		go_bigquery.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.CustomerPONumber,
		c.Description,
		c.Division,
		c.DivisionName,
		go_bigquery.DateToNullTimestamp(c.EndDate),
		c.FixedPriceItem.String(),
		c.FixedPriceItemDescription,
		c.HasWBSLines,
		c.InternalNotes,
		c.InvoiceAsQuoted,
		c.Manager.String(),
		c.ManagerFullname,
		c.MarkupPercentage,
		go_bigquery.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.Notes,
		c.PrepaidItem.String(),
		c.PrepaidItemDescription,
		c.PrepaidType,
		c.PrepaidTypeDescription,
		c.SalesTimeQuantity,
		c.SourceQuotation.String(),
		go_bigquery.DateToNullTimestamp(c.StartDate),
		c.TimeQuantityToAlert,
		c.Type,
		c.TypeDescription,
		c.UseBillingMilestones,
	}
}

func (service *Service) WriteProjects(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.ProjectService().NewGetProjectsCall(lastModified)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		projects, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if projects == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *projects {
			batchRowCount++

			b, err := json.Marshal(getProject(&tl, organisationID, softwareClientLicenceID, softwareClientLicenseGuid))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#Projects flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#Projects: %v\n", rowCount)

	return objectHandles, rowCount, Project{}, nil
}
